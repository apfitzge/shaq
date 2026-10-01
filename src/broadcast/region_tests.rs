use super::*;

fn config() -> BroadcastConfig {
    BroadcastConfig {
        capacity: 3,
        producer_slots: 2,
        consumer_slots: 2,
    }
}

#[test]
fn public_layout_uses_rounded_capacity_and_section_alignment() {
    let layout = config().layout::<u64>().unwrap();
    let rounded = BroadcastConfig {
        capacity: 4,
        ..config()
    };
    assert_eq!(layout, rounded.layout::<u64>().unwrap());
    assert_eq!(
        layout,
        config().layout_for_payload(Layout::new::<u64>()).unwrap()
    );
    assert_eq!(
        layout.size(),
        QueueLayout::new::<u64>(&config()).unwrap().total
    );
    assert_eq!(layout.align(), 64);
    assert_eq!(layout.size() % layout.align(), 0);
    assert!(config().layout::<()>().is_ok());
    assert!(BroadcastConfig {
        consumer_slots: 0,
        ..config()
    }
    .layout::<u64>()
    .is_ok());
}

#[test]
fn public_layout_rejects_invalid_or_unaddressable_sizes() {
    for config in [
        BroadcastConfig {
            capacity: 0,
            ..config()
        },
        BroadcastConfig {
            capacity: usize::MAX,
            ..config()
        },
        BroadcastConfig {
            producer_slots: 0,
            ..config()
        },
        BroadcastConfig {
            producer_slots: usize::MAX,
            ..config()
        },
        BroadcastConfig {
            consumer_slots: usize::MAX,
            ..config()
        },
    ] {
        assert!(matches!(
            config.layout::<u64>(),
            Err(Error::InvalidBufferSize)
        ));
    }
    let over_aligned = Layout::from_size_align(128, 128).unwrap();
    assert!(matches!(
        config().layout_for_payload(over_aligned),
        Err(Error::InvalidBufferSize)
    ));

    // Arithmetic can fit usize while the complete queue exceeds isize::MAX.
    let huge_payload = Layout::from_size_align(isize::MAX as usize / 2, 1).unwrap();
    let config = BroadcastConfig {
        capacity: 1,
        producer_slots: 3,
        ..config()
    };
    assert!(matches!(
        config.layout_for_payload(huge_payload),
        Err(Error::InvalidBufferSize)
    ));
}

#[test]
fn checked_view_rejects_tiny_overflowing_and_misaligned_ranges() {
    let tiny = Region::alloc(NonZeroUsize::MIN).unwrap();
    assert!(matches!(
        // SAFETY: zeroed allocation; validation must reject its length before reading a header.
        unsafe { SharedQueue::join_region::<u64>(&tiny) },
        Err(Error::InvalidBufferSize)
    ));
    let region = Region::alloc(NonZeroUsize::new(4096).unwrap()).unwrap();
    for (offset, extent) in [
        (0, 0),
        (0, size_of::<SharedQueueHeader>() - 1),
        (4096, 4096),
        (usize::MAX, 2),
    ] {
        assert!(matches!(
            QueueRegion::new(Arc::clone(&region), offset, extent),
            Err(Error::InvalidBufferSize)
        ));
    }
    assert!(matches!(
        QueueRegion::new(region, 1, 1024),
        Err(Error::InvalidRegionAlignment { .. })
    ));
}

#[test]
fn relocated_heap_view_retains_owner_and_supports_zero_sized_payloads() {
    let layout = config().layout::<()>().unwrap();
    let offset = layout.align();
    let owner = Region::alloc(NonZeroUsize::new(offset + layout.size()).unwrap()).unwrap();
    let weak = Arc::downgrade(&owner);
    let view = QueueRegion::new(owner, offset, layout.size()).unwrap();
    // SAFETY: fresh aligned allocation, initialized once within the checked view.
    let queue = unsafe {
        SharedQueue::create_in_view(
            view,
            QueueLayout::new::<()>(&config()).unwrap(),
            DEFAULT_QUEUE_IDENTIFIER,
        )
    }
    .unwrap();
    let broadcast = Broadcast::<()>::from_queue(queue);
    let mut producer = broadcast.producer(ProducerId::new(1)).unwrap();
    let mut consumer = broadcast.consumer().unwrap();
    drop(broadcast);
    assert!(producer.try_write(()).is_ok());
    let guard = consumer.try_reserve_read().unwrap();
    drop(producer);
    assert!(weak.upgrade().is_some());
    assert_eq!(*guard.as_ref(), ());
    drop(guard);
    drop(consumer);
    assert!(weak.upgrade().is_none());
}

#[cfg(not(miri))]
mod file {
    use super::*;
    use crate::shmem::create_temp_shmem_file;
    use std::io::{Read, Seek, SeekFrom, Write};

    const SENTINEL: u8 = 0xa5;

    fn filled_file(size: usize) -> File {
        let mut file = create_temp_shmem_file().unwrap();
        file.write_all(&vec![SENTINEL; size]).unwrap();
        file
    }

    fn bytes(file: &File, offset: usize, size: usize) -> Vec<u8> {
        let mut file = file.try_clone().unwrap();
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        let mut bytes = vec![0; size];
        file.read_exact(&mut bytes).unwrap();
        bytes
    }

    #[test]
    fn custom_identifier_survives_relocated_typed_and_untyped_joins() {
        let layout = config().layout::<u64>().unwrap();
        let offset = layout.align() as u64;
        let extent = layout.size() as u64;
        let identifier = 0x1234_5678_9abc_def0;
        let file = filled_file(layout.align() + layout.size());
        // SAFETY: fresh aligned queue storage with portable u64 payloads, initialized once.
        let queue = unsafe {
            Broadcast::<u64>::create_at_with_identifier(&file, offset, extent, config(), identifier)
        }
        .unwrap();
        assert_eq!(queue.queue_identifier(), identifier);
        // SAFETY: same live u64 queue and bounds; the file is not resized.
        let typed = unsafe { Broadcast::<u64>::join_at(&file, offset, extent) }.unwrap();
        assert_eq!(typed.queue_identifier(), identifier);
        // SAFETY: same live queue; u64 has no uninitialized padding.
        let untyped = unsafe { Broadcast::join_untyped_at(&file, offset, extent) }.unwrap();
        assert_eq!(untyped.queue_identifier(), identifier);
    }

    #[test]
    fn region_roundtrip_preserves_surroundings_and_mapping_lifetimes() {
        let layout = config().layout::<u64>().unwrap();
        for offset in [0, layout.align(), 3 * layout.align(), 4096 + layout.align()] {
            let extent = layout.size() + 29;
            let size = offset + extent + 47;
            let file = filled_file(size);
            // SAFETY: fresh, nonoverlapping queue storage, with a portable u64 payload.
            let creator = unsafe {
                Broadcast::<u64>::create_at(&file, offset as u64, extent as u64, config())
            }
            .unwrap();
            assert_eq!(file.metadata().unwrap().len(), size as u64);
            assert_eq!(bytes(&file, 0, offset), vec![SENTINEL; offset]);
            assert_eq!(
                bytes(&file, offset + layout.size(), size - offset - layout.size()),
                vec![SENTINEL; size - offset - layout.size()]
            );

            let clone = creator.clone();
            let mut producer = clone.producer(ProducerId::new(1)).unwrap();
            let other_file = file.try_clone().unwrap();
            // SAFETY: live u64 queue, mapped independently with the same bounds.
            let typed =
                unsafe { Broadcast::<u64>::join_at(&other_file, offset as u64, extent as u64) }
                    .unwrap();
            let mut consumer = typed.consumer().unwrap();
            // SAFETY: u64 initializes every payload byte in this live queue.
            let untyped =
                unsafe { Broadcast::join_untyped_at(&file, offset as u64, extent as u64) }.unwrap();
            // SAFETY: the u64 payload has no uninitialized padding.
            let mut slices = unsafe { untyped.slice_consumer() }.unwrap();
            drop((creator, clone, typed, untyped, other_file));

            // Exercise held write/read guards after dropping the parent handles.
            // SAFETY: the guard is initialized before publication below.
            let guard = unsafe { producer.try_reserve_write() }.unwrap();
            guard.write(42);
            let read = consumer.try_reserve_read().unwrap();
            let slice = slices.try_read().unwrap();
            drop(producer);
            assert_eq!(*read.as_ref(), 42);
            assert_eq!(slice.as_slice(), 42u64.to_ne_bytes());
            drop((read, slice));
            drop((consumer, slices));

            assert_eq!(file.metadata().unwrap().len(), size as u64);
            assert_eq!(bytes(&file, 0, offset), vec![SENTINEL; offset]);
            assert_eq!(
                bytes(&file, offset + layout.size(), size - offset - layout.size()),
                vec![SENTINEL; size - offset - layout.size()]
            );
        }
    }

    #[test]
    fn independent_queues_share_one_file() {
        let layout = config().layout::<u64>().unwrap();
        let first_offset = layout.align();
        let second_offset = first_offset + layout.size() + layout.align();
        let size = second_offset + layout.size() + layout.align();
        let file = filled_file(size);
        // SAFETY: fresh aligned storage for the first queue.
        let first = unsafe {
            Broadcast::<u64>::create_at(&file, first_offset as u64, layout.size() as u64, config())
        }
        .unwrap();
        let mut p0 = first.producer(ProducerId::new(1)).unwrap();
        let mut c0 = first.consumer().unwrap();
        assert!(p0.try_write(11).is_ok());
        // SAFETY: the second queue does not overlap the live first queue.
        let second = unsafe {
            Broadcast::<[u8; 8]>::create_at(
                &file,
                second_offset as u64,
                layout.size() as u64,
                config(),
            )
        }
        .unwrap();
        let mut p1 = second.producer(ProducerId::new(1)).unwrap();
        let mut c1 = second.consumer().unwrap();
        assert!(p1.try_write([22; 8]).is_ok());
        assert_eq!(c0.try_read(), Some(11));
        assert_eq!(c1.try_read(), Some([22; 8]));
        assert_eq!(c0.try_read(), None);
        assert_eq!(c1.try_read(), None);
        assert_eq!(file.metadata().unwrap().len(), size as u64);
        for offset in [
            0,
            first_offset + layout.size(),
            second_offset + layout.size(),
        ] {
            assert_eq!(
                bytes(&file, offset, layout.align()),
                vec![SENTINEL; layout.align()]
            );
        }
    }

    #[test]
    fn zero_offset_apis_remain_interoperable() {
        let layout = config().layout::<u64>().unwrap();
        let file = filled_file(layout.size() * 2);
        // SAFETY: fresh file with no live queue; legacy creation may resize it.
        let creator = unsafe { Broadcast::<u64>::create(&file, config()) }.unwrap();
        assert_eq!(file.metadata().unwrap().len(), layout.size() as u64);
        // SAFETY: same live queue and payload type.
        let bounded = unsafe { Broadcast::<u64>::join_at(&file, 0, layout.size() as u64) }.unwrap();
        let mut producer = creator.producer(ProducerId::new(1)).unwrap();
        let mut consumer = bounded.consumer().unwrap();
        assert!(producer.try_write(1).is_ok());
        assert_eq!(consumer.try_read(), Some(1));

        let file = filled_file(layout.size() * 2);
        // SAFETY: fresh queue region at offset zero; excess file space is preserved.
        let creator =
            unsafe { Broadcast::<u64>::create_at(&file, 0, layout.size() as u64, config()) }
                .unwrap();
        // SAFETY: live u64 queue at the start of the file.
        let legacy = unsafe { Broadcast::<u64>::join(&file) }.unwrap();
        // SAFETY: live queue with fully initialized u64 payload bytes.
        let untyped = unsafe { Broadcast::join_untyped(&file) }.unwrap();
        let mut producer = creator.producer(ProducerId::new(1)).unwrap();
        let mut consumer = legacy.consumer().unwrap();
        // SAFETY: u64 initializes every byte.
        let mut slices = unsafe { untyped.slice_consumer() }.unwrap();
        assert!(producer.try_write(2).is_ok());
        assert_eq!(consumer.try_read(), Some(2));
        assert_eq!(slices.try_read().unwrap().as_slice(), 2u64.to_ne_bytes());
        assert_eq!(file.metadata().unwrap().len(), (layout.size() * 2) as u64);
    }

    #[test]
    fn invalid_ranges_fail_without_modifying_the_file() {
        let layout = config().layout::<u64>().unwrap();
        let size = layout.size() * 2;
        let file = filled_file(size);
        for (offset, extent) in [
            (0, 0),
            (0, (size_of::<SharedQueueHeader>() - 1) as u64),
            (size as u64, layout.size() as u64),
            (size as u64 + 1, layout.size() as u64),
            (0, size as u64 + 1),
            (u64::MAX, layout.size() as u64),
            (layout.align() as u64, u64::MAX),
        ] {
            // SAFETY: no live queue; invalid ranges must be rejected before access.
            let create = unsafe { Broadcast::<u64>::create_at(&file, offset, extent, config()) };
            assert!(matches!(create, Err(Error::InvalidBufferSize)));
            // SAFETY: invalid bounds are rejected before dereferencing the header.
            let typed = unsafe { Broadcast::<u64>::join_at(&file, offset, extent) };
            assert!(matches!(typed, Err(Error::InvalidBufferSize)));
            // SAFETY: invalid bounds are rejected before dereferencing the header.
            let untyped = unsafe { Broadcast::join_untyped_at(&file, offset, extent) };
            assert!(matches!(untyped, Err(Error::InvalidBufferSize)));
        }
        for offset in [1, layout.align() - 1, layout.align() + 1] {
            // SAFETY: misalignment must be rejected before initialization or header access.
            let create = unsafe {
                Broadcast::<u64>::create_at(&file, offset as u64, layout.size() as u64, config())
            };
            assert!(matches!(create, Err(Error::InvalidRegionAlignment { .. })));
            // SAFETY: misalignment is rejected before header access.
            let typed =
                unsafe { Broadcast::<u64>::join_at(&file, offset as u64, layout.size() as u64) };
            assert!(matches!(typed, Err(Error::InvalidRegionAlignment { .. })));
            // SAFETY: misalignment is rejected before header access.
            let untyped =
                unsafe { Broadcast::join_untyped_at(&file, offset as u64, layout.size() as u64) };
            assert!(matches!(untyped, Err(Error::InvalidRegionAlignment { .. })));
        }
        assert_eq!(file.metadata().unwrap().len(), size as u64);
        assert_eq!(bytes(&file, 0, size), vec![SENTINEL; size]);
    }

    #[test]
    fn queue_must_fit_declared_extent_even_when_it_fits_file() {
        let layout = config().layout::<u64>().unwrap();
        let offset = layout.align() as u64;
        let extent = layout.size() as u64;
        let file = filled_file(layout.align() + layout.size() * 2);
        // SAFETY: fresh storage; too-small extent must fail without writing.
        let result = unsafe { Broadcast::<u64>::create_at(&file, offset, extent - 1, config()) };
        assert!(matches!(result, Err(Error::InvalidBufferSize)));
        assert_eq!(
            bytes(&file, offset as usize, layout.size()),
            vec![SENTINEL; layout.size()]
        );
        // SAFETY: fresh, properly sized queue region.
        let queue =
            unsafe { Broadcast::<u64>::create_at(&file, offset, extent, config()) }.unwrap();
        // SAFETY: live queue; bounds validation rejects the truncated view.
        let typed = unsafe { Broadcast::<u64>::join_at(&file, offset, extent - 1) };
        assert!(matches!(typed, Err(Error::InvalidBufferSize)));
        // SAFETY: same live queue; bounds validation rejects the truncated view.
        let untyped = unsafe { Broadcast::join_untyped_at(&file, offset, extent - 1) };
        assert!(matches!(untyped, Err(Error::InvalidBufferSize)));
        // SAFETY: live queue; the requested type has the wrong alignment.
        let wrong_type = unsafe { Broadcast::<[u8; 8]>::join_at(&file, offset, extent) };
        assert!(matches!(wrong_type, Err(Error::InvalidBufferSize)));
        drop(queue);
    }

    #[test]
    fn tiny_and_truncated_files_are_rejected() {
        for size in [0, 1, size_of::<SharedQueueHeader>() - 1] {
            let file = filled_file(size);
            assert!(matches!(
                // SAFETY: tiny files must be rejected before mapping/header access.
                unsafe { Broadcast::<u64>::join(&file) },
                Err(Error::InvalidBufferSize)
            ));
            assert!(matches!(
                // SAFETY: tiny files must be rejected before mapping/header access.
                unsafe { Broadcast::join_untyped(&file) },
                Err(Error::InvalidBufferSize)
            ));
        }
        let layout = config().layout::<u64>().unwrap();
        let file = filled_file(layout.size());
        // SAFETY: fresh queue storage.
        let queue = unsafe { Broadcast::<u64>::create(&file, config()) }.unwrap();
        drop(queue);
        // No mappings remain alive while truncating the file.
        file.set_len((layout.size() - 1) as u64).unwrap();
        assert!(matches!(
            // SAFETY: the header is intact; layout validation must reject the missing tail.
            unsafe { Broadcast::<u64>::join(&file) },
            Err(Error::InvalidBufferSize)
        ));
        assert!(matches!(
            // SAFETY: the header is intact; layout validation must reject the missing tail.
            unsafe { Broadcast::join_untyped(&file) },
            Err(Error::InvalidBufferSize)
        ));
        assert!(matches!(
            // SAFETY: the original extent now exceeds the file and must be rejected.
            unsafe { Broadcast::<u64>::join_at(&file, 0, layout.size() as u64) },
            Err(Error::InvalidBufferSize)
        ));
    }

    #[test]
    fn relocated_join_validates_magic_version_and_stored_layout() {
        use std::mem::offset_of;
        let corruptions = [
            (
                offset_of!(SharedQueueHeader, magic),
                0u64.to_ne_bytes().to_vec(),
            ),
            (
                offset_of!(SharedQueueHeader, version),
                (VERSION + 1).to_ne_bytes().to_vec(),
            ),
            (
                offset_of!(SharedQueueHeader, capacity),
                0u32.to_ne_bytes().to_vec(),
            ),
            (
                offset_of!(SharedQueueHeader, capacity),
                3u32.to_ne_bytes().to_vec(),
            ),
            (
                offset_of!(SharedQueueHeader, capacity),
                (1u32 << 31).to_ne_bytes().to_vec(),
            ),
            (
                offset_of!(SharedQueueHeader, producer_slots),
                0u32.to_ne_bytes().to_vec(),
            ),
            (
                offset_of!(SharedQueueHeader, consumer_slots),
                u32::MAX.to_ne_bytes().to_vec(),
            ),
            (
                offset_of!(SharedQueueHeader, payload_size),
                usize::MAX.to_ne_bytes().to_vec(),
            ),
            (
                offset_of!(SharedQueueHeader, payload_align),
                0usize.to_ne_bytes().to_vec(),
            ),
            (
                offset_of!(SharedQueueHeader, payload_align),
                3usize.to_ne_bytes().to_vec(),
            ),
            (
                offset_of!(SharedQueueHeader, payload_align),
                128usize.to_ne_bytes().to_vec(),
            ),
        ];
        let layout = config().layout::<u64>().unwrap();
        for (field, replacement) in corruptions {
            let offset = layout.align();
            let mut file = filled_file(offset + layout.size());
            // SAFETY: fresh nonoverlapping queue storage.
            let queue = unsafe {
                Broadcast::<u64>::create_at(&file, offset as u64, layout.size() as u64, config())
            }
            .unwrap();
            drop(queue);
            // No mappings or endpoints remain while changing the header fixture.
            file.seek(SeekFrom::Start((offset + field) as u64)).unwrap();
            file.write_all(&replacement).unwrap();
            // SAFETY: header bytes are initialized; the corrupted metadata must
            // be rejected before constructing any lane views.
            let typed =
                unsafe { Broadcast::<u64>::join_at(&file, offset as u64, layout.size() as u64) };
            // SAFETY: same fully initialized, deliberately invalid header fixture.
            let untyped =
                unsafe { Broadcast::join_untyped_at(&file, offset as u64, layout.size() as u64) };
            for result in [typed.map(|_| ()), untyped.map(|_| ())] {
                if field == offset_of!(SharedQueueHeader, magic) {
                    assert!(matches!(result, Err(Error::InvalidMagic)));
                } else if field == offset_of!(SharedQueueHeader, version) {
                    assert!(matches!(result, Err(Error::InvalidVersion { .. })));
                } else {
                    assert!(
                        matches!(result, Err(Error::InvalidBufferSize)),
                        "field {field}"
                    );
                }
            }
        }
    }
}
