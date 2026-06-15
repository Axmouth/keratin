use std::collections::{HashMap, VecDeque};

use keratin_log::Message;

use crate::{Offset, event::StromaEvent};

const MESSAGE_OVERHEAD_BYTES: usize = 16;
const EVENT_OVERHEAD_BYTES: usize = 16;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct ReplicationCacheKey {
    topic: Box<str>,
    part: u32,
    group: Option<Box<str>>,
}

impl ReplicationCacheKey {
    pub(crate) fn from_parts(topic: &str, part: u32, group: Option<&str>) -> Self {
        Self {
            topic: topic.into(),
            part,
            group: group.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CachedStream {
    Messages,
    Events,
}

#[derive(Debug, Clone)]
struct CachedRef {
    key: ReplicationCacheKey,
    stream: CachedStream,
    offset: Offset,
}

#[derive(Debug, Clone)]
struct CachedRecord<T> {
    offset: Offset,
    bytes: usize,
    record: T,
}

#[derive(Debug, Clone)]
pub(crate) struct ReplicationCacheRead<T> {
    pub(crate) requested_offset: Offset,
    pub(crate) next_offset: Offset,
    pub(crate) records: Vec<(Offset, T)>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReplicationCacheMutation {
    pub(crate) retained_bytes: usize,
    pub(crate) evicted_records: usize,
}

#[derive(Debug, Clone)]
struct QueueCache {
    messages: OffsetSuffix<Message>,
    events: OffsetSuffix<StromaEvent>,
}

impl Default for QueueCache {
    fn default() -> Self {
        Self {
            messages: OffsetSuffix::default(),
            events: OffsetSuffix::default(),
        }
    }
}

impl QueueCache {
    fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.events.is_empty()
    }

    fn evict(&mut self, stream: CachedStream, offset: Offset) -> Option<usize> {
        match stream {
            CachedStream::Messages => self.messages.evict_offset(offset),
            CachedStream::Events => self.events.evict_offset(offset),
        }
    }
}

#[derive(Debug, Clone)]
struct OffsetSuffix<T> {
    records: VecDeque<CachedRecord<T>>,
}

impl<T> Default for OffsetSuffix<T> {
    fn default() -> Self {
        Self {
            records: VecDeque::new(),
        }
    }
}

impl<T> OffsetSuffix<T> {
    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn insert(&mut self, offset: Offset, record: T, bytes: usize) -> ReplicationCacheMutation {
        let Some(last) = self.records.back() else {
            self.records.push_back(CachedRecord {
                offset,
                bytes,
                record,
            });
            return ReplicationCacheMutation::default();
        };

        let mut mutation = ReplicationCacheMutation::default();
        if offset != last.offset.saturating_add(1) {
            mutation.retained_bytes = self.records.iter().map(|record| record.bytes).sum();
            mutation.evicted_records = self.records.len();
            self.records.clear();
        }

        self.records.push_back(CachedRecord {
            offset,
            bytes,
            record,
        });
        mutation
    }

    fn read(&self, from: Offset, max: usize) -> Option<ReplicationCacheRead<T>>
    where
        T: Clone,
    {
        if max == 0 {
            return Some(ReplicationCacheRead {
                requested_offset: from,
                next_offset: from,
                records: Vec::new(),
            });
        }

        let front = self.records.front()?;
        if from < front.offset {
            return None;
        }

        let index = usize::try_from(from - front.offset).ok()?;
        if index >= self.records.len() {
            return None;
        }

        let mut out = Vec::new();
        let mut next_offset = from;
        for item in self.records.iter().skip(index).take(max) {
            out.push((item.offset, item.record.clone()));
            next_offset = item.offset.saturating_add(1);
        }

        Some(ReplicationCacheRead {
            requested_offset: from,
            next_offset,
            records: out,
        })
    }

    fn evict_offset(&mut self, offset: Offset) -> Option<usize> {
        let front = self.records.front()?;
        if front.offset != offset {
            return None;
        }
        self.records.pop_front().map(|record| record.bytes)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RecentReplicationCache {
    max_bytes: usize,
    total_bytes: usize,
    order: VecDeque<CachedRef>,
    queues: HashMap<ReplicationCacheKey, QueueCache>,
}

impl RecentReplicationCache {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            total_bytes: 0,
            order: VecDeque::new(),
            queues: HashMap::new(),
        }
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub(crate) fn insert_messages<I>(
        &mut self,
        key: &ReplicationCacheKey,
        first_offset: Offset,
        records: I,
    ) -> ReplicationCacheMutation
    where
        I: IntoIterator<Item = Message>,
    {
        let mut evicted_records = 0;
        for (index, record) in records.into_iter().enumerate() {
            let offset = first_offset.saturating_add(index as u64);
            evicted_records += self.insert_message(key, offset, record);
        }
        ReplicationCacheMutation {
            retained_bytes: self.total_bytes,
            evicted_records,
        }
    }

    pub(crate) fn insert_events<I>(
        &mut self,
        key: &ReplicationCacheKey,
        first_offset: Offset,
        records: I,
    ) -> ReplicationCacheMutation
    where
        I: IntoIterator<Item = StromaEvent>,
    {
        let mut evicted_records = 0;
        for (index, record) in records.into_iter().enumerate() {
            let offset = first_offset.saturating_add(index as u64);
            evicted_records += self.insert_event(key, offset, record);
        }
        ReplicationCacheMutation {
            retained_bytes: self.total_bytes,
            evicted_records,
        }
    }

    pub(crate) fn read_messages(
        &self,
        key: &ReplicationCacheKey,
        from: Offset,
        max: usize,
    ) -> Option<ReplicationCacheRead<Message>> {
        self.queues.get(key)?.messages.read(from, max)
    }

    pub(crate) fn read_events(
        &self,
        key: &ReplicationCacheKey,
        from: Offset,
        max: usize,
    ) -> Option<ReplicationCacheRead<StromaEvent>> {
        self.queues.get(key)?.events.read(from, max)
    }

    fn insert_message(
        &mut self,
        key: &ReplicationCacheKey,
        offset: Offset,
        record: Message,
    ) -> usize {
        let bytes = message_bytes(&record);
        self.insert_record(key, CachedStream::Messages, offset, bytes, |queue| {
            queue.messages.insert(offset, record, bytes)
        })
    }

    fn insert_event(
        &mut self,
        key: &ReplicationCacheKey,
        offset: Offset,
        record: StromaEvent,
    ) -> usize {
        let bytes = event_bytes(&record);
        self.insert_record(key, CachedStream::Events, offset, bytes, |queue| {
            queue.events.insert(offset, record, bytes)
        })
    }

    fn insert_record(
        &mut self,
        key: &ReplicationCacheKey,
        stream: CachedStream,
        offset: Offset,
        bytes: usize,
        insert: impl FnOnce(&mut QueueCache) -> ReplicationCacheMutation,
    ) -> usize {
        if self.max_bytes == 0 || bytes > self.max_bytes {
            return 0;
        }

        let queue = self.queues.entry(key.clone()).or_default();
        let mutation = insert(queue);
        self.total_bytes = self.total_bytes.saturating_sub(mutation.retained_bytes);
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.order.push_back(CachedRef {
            key: key.clone(),
            stream,
            offset,
        });
        mutation.evicted_records + self.evict_to_budget()
    }

    fn evict_to_budget(&mut self) -> usize {
        let mut evicted_records = 0;
        while self.total_bytes > self.max_bytes {
            let Some(entry) = self.order.pop_front() else {
                self.total_bytes = 0;
                return evicted_records;
            };

            let mut remove_queue = false;
            if let Some(queue) = self.queues.get_mut(&entry.key) {
                if let Some(bytes) = queue.evict(entry.stream, entry.offset) {
                    self.total_bytes = self.total_bytes.saturating_sub(bytes);
                    evicted_records += 1;
                }
                remove_queue = queue.is_empty();
            }
            if remove_queue {
                self.queues.remove(&entry.key);
            }
        }
        evicted_records
    }
}

fn message_bytes(message: &Message) -> usize {
    MESSAGE_OVERHEAD_BYTES
        .saturating_add(message.headers.len())
        .saturating_add(message.payload.len())
}

fn event_bytes(event: &StromaEvent) -> usize {
    EVENT_OVERHEAD_BYTES.saturating_add(
        event
            .encode()
            .map(|bytes| bytes.len())
            .unwrap_or(std::mem::size_of_val(event)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(topic: &str) -> ReplicationCacheKey {
        ReplicationCacheKey::from_parts(topic, 0, None)
    }

    fn message(payload: &[u8]) -> Message {
        Message {
            flags: 0,
            headers: Vec::new(),
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn reads_contiguous_message_suffix_by_queue() {
        let key_a = key("a");
        let key_b = key("b");
        let mut cache = RecentReplicationCache::new(1024);

        cache.insert_messages(
            &key_a,
            10,
            vec![message(b"a"), message(b"b"), message(b"c")],
        );
        cache.insert_messages(&key_b, 10, vec![message(b"x")]);

        let read = match cache.read_messages(&key_a, 11, 10) {
            Some(read) => read,
            None => panic!("expected cached read"),
        };
        assert_eq!(read.requested_offset, 11);
        assert_eq!(read.next_offset, 13);
        assert_eq!(read.records.len(), 2);
        assert_eq!(read.records[0].0, 11);
        assert_eq!(read.records[0].1.payload, b"b");
        assert_eq!(read.records[1].0, 12);
        assert_eq!(read.records[1].1.payload, b"c");

        let other = match cache.read_messages(&key_b, 10, 10) {
            Some(read) => read,
            None => panic!("expected cached read"),
        };
        assert_eq!(other.records.len(), 1);
        assert_eq!(other.records[0].1.payload, b"x");
    }

    #[test]
    fn read_limit_caps_records_without_eviction() {
        let key = key("a");
        let mut cache = RecentReplicationCache::new(1024);
        cache.insert_messages(&key, 0, vec![message(b"a"), message(b"b"), message(b"c")]);

        let read = match cache.read_messages(&key, 0, 2) {
            Some(read) => read,
            None => panic!("expected cached read"),
        };
        assert_eq!(read.next_offset, 2);
        assert_eq!(read.records.len(), 2);
    }

    #[test]
    fn global_byte_budget_evicts_oldest_records_across_queues() {
        let key_a = key("a");
        let key_b = key("b");
        let mut cache = RecentReplicationCache::new(36);

        cache.insert_messages(&key_a, 0, vec![message(b"aaaa")]);
        cache.insert_messages(&key_b, 0, vec![message(b"bbbb")]);

        assert!(cache.total_bytes() <= 36);
        assert!(cache.read_messages(&key_a, 0, 1).is_none());
        assert!(cache.read_messages(&key_b, 0, 1).is_some());
    }

    #[test]
    fn gap_resets_suffix_and_old_offsets_miss() {
        let key = key("a");
        let mut cache = RecentReplicationCache::new(1024);

        cache.insert_messages(&key, 0, vec![message(b"a"), message(b"b")]);
        assert_eq!(cache.total_bytes(), 34);
        cache.insert_messages(&key, 5, vec![message(b"c")]);

        assert_eq!(cache.total_bytes(), 17);
        assert!(cache.read_messages(&key, 0, 1).is_none());
        let read = match cache.read_messages(&key, 5, 1) {
            Some(read) => read,
            None => panic!("expected cached read"),
        };
        assert_eq!(read.records[0].1.payload, b"c");

        cache.insert_messages(&key, 6, vec![message(b"d")]);
        assert_eq!(cache.total_bytes(), 34);
        let read = match cache.read_messages(&key, 5, 2) {
            Some(read) => read,
            None => panic!("expected cached read after gap reset"),
        };
        assert_eq!(read.records.len(), 2);
    }

    #[test]
    fn oversized_record_is_not_cached() {
        let key = key("a");
        let mut cache = RecentReplicationCache::new(8);

        cache.insert_messages(&key, 0, vec![message(b"this is too large")]);

        assert_eq!(cache.total_bytes(), 0);
        assert!(cache.read_messages(&key, 0, 1).is_none());
    }

    #[test]
    fn events_share_the_same_global_budget() {
        let key_a = key("a");
        let key_b = key("b");
        let mut cache = RecentReplicationCache::new(80);

        cache.insert_events(&key_a, 0, vec![StromaEvent::Ack { off: 1 }]);
        cache.insert_messages(&key_b, 0, vec![message(b"large-enough-to-evict-the-event")]);

        assert!(cache.total_bytes() <= 80);
        assert!(cache.read_events(&key_a, 0, 1).is_none());
        assert!(cache.read_messages(&key_b, 0, 1).is_some());
    }

    #[test]
    fn cache_does_not_answer_at_tail_for_positive_reads() {
        let key = key("a");
        let mut cache = RecentReplicationCache::new(1024);

        cache.insert_messages(&key, 10, vec![message(b"a")]);

        assert!(cache.read_messages(&key, 11, 1).is_none());
        assert!(cache.read_messages(&key, 11, 0).is_some());
    }
}
