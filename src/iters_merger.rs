use std::{
    ops::{
        DerefMut,
    },
    cmp::{
        Ordering,
    },
    collections::{
        BinaryHeap,
    },
};

pub struct Cps<V, S, T> where T: ComparableItem {
    state: State,
    env: Env<V, S, T>,
}

pub trait ComparableItem {
    fn compare_primary(&self, other: &Self) -> Ordering;
    fn compare_secondary(&self, other: &Self) -> Ordering;
}

pub enum Kont<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    ScheduleIterAwait(KontScheduleIterAwait<V, S, T>),
    AwaitScheduled(KontAwaitScheduled<V, S, T>),
    EmitDeprecated(KontEmitDeprecated<V, S, T>),
    EmitItem(KontEmitItem<V, S, T>),
    Finished,
}

pub struct KontScheduleIterAwait<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    pub await_iter: S,
    pub next: KontScheduleIterAwaitNext<V, S, T>,
}

pub struct KontScheduleIterAwaitNext<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    state_await: StateAwait,
    env: Env<V, S, T>,
}

pub struct KontAwaitScheduled<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    pub next: KontAwaitScheduledNext<V, S, T>,
}

pub struct KontAwaitScheduledNext<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    state_await: StateAwait,
    env: Env<V, S, T>,
}

pub struct KontEmitDeprecated<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    pub item: T,
    pub next: KontEmitDeprecatedNext<V, S, T>,
}

pub struct KontEmitDeprecatedNext<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    env: Env<V, S, T>,
}

pub struct KontEmitItem<V, S, T> where V: DerefMut<Target = Vec<S>> , T: ComparableItem {
    pub item: T,
    pub next: KontEmitItemNext<V, S, T>,
}

pub struct KontEmitItemNext<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    env: Env<V, S, T>,
}

struct Env<V, S, T> where T: ComparableItem {
    iters: V,
    fronts: BinaryHeap<Front<S, T>>,
    candidate: Option<T>,
}

struct Front<S, T> {
    front_item: T,
    iter: S,
}

enum State {
    Await(StateAwait),
    Flush,
}

#[derive(Default)]
struct StateAwait {
    pending_count: usize,
}

impl<V, S, T> Cps<V, S, T> where T: ComparableItem {
    pub fn new(iters: V) -> Self {
        Self {
            state: State::Await(StateAwait::default()),
            env: Env {
                iters,
                fronts: BinaryHeap::new(),
                candidate: None,
            },
        }
    }
}

impl<V, S, T> Cps<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    pub fn step(mut self) -> Kont<V, S, T> {
        loop {
            match self.state {

                State::Await(StateAwait { pending_count, }) if !self.env.iters.is_empty() => {
                    let await_iter = self.env.iters.pop().unwrap();
                    return Kont::ScheduleIterAwait(KontScheduleIterAwait {
                        await_iter,
                        next: KontScheduleIterAwaitNext {
                            state_await: StateAwait {
                                pending_count: pending_count + 1,
                            },
                            env: self.env,
                        },
                    });
                },

                State::Await(StateAwait { pending_count, }) if pending_count == 0 =>
                    self.state = State::Flush,

                State::Await(StateAwait { pending_count, }) =>
                    return Kont::AwaitScheduled(KontAwaitScheduled {
                        next: KontAwaitScheduledNext {
                            state_await: StateAwait { pending_count, },
                            env: self.env,
                        },
                    }),

                State::Flush =>
                    match self.env.candidate.take() {
                        None =>
                            match self.env.fronts.pop() {
                                None if self.env.iters.is_empty() =>
                                    return Kont::Finished,
                                None =>
                                    self.state = State::Await(StateAwait::default()),
                                Some(Front { front_item, iter, }) => {
                                    self.env.candidate = Some(front_item);
                                    self.env.iters.push(iter);
                                },
                            },
                        Some(candidate_item) =>
                            match self.env.fronts.peek() {
                                None if self.env.iters.is_empty() =>
                                    return Kont::EmitItem(KontEmitItem {
                                        item: candidate_item,
                                        next: KontEmitItemNext { env: self.env, },
                                    }),
                                None => {
                                    self.env.candidate = Some(candidate_item);
                                    self.state = State::Await(StateAwait::default());
                                },
                                Some(Front { front_item, .. }) =>
                                    if candidate_item.compare_primary(front_item) == Ordering::Equal {
                                        let should_swap = front_item.compare_secondary(&candidate_item) == Ordering::Less;
                                        let Front { front_item: next_front_item, iter, } =
                                            self.env.fronts.pop().unwrap();
                                        let (deprecated_item, next_candidate_item) =
                                            if should_swap {
                                                (next_front_item, candidate_item)
                                            } else {
                                                (candidate_item, next_front_item)
                                            };
                                        self.env.candidate = Some(next_candidate_item);
                                        self.env.iters.push(iter);
                                        return Kont::EmitDeprecated(KontEmitDeprecated {
                                            item: deprecated_item,
                                            next: KontEmitDeprecatedNext { env: self.env, },
                                        });
                                    } else if self.env.iters.is_empty() {
                                        return Kont::EmitItem(KontEmitItem {
                                            item: candidate_item,
                                            next: KontEmitItemNext { env: self.env, },
                                        });
                                    } else {
                                        self.env.candidate = Some(candidate_item);
                                        self.state = State::Await(StateAwait::default());
                                    },
                            },
                    },

            }
        }
    }
}

impl<V, S, T> From<KontScheduleIterAwaitNext<V, S, T>> for Cps<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    fn from(kont: KontScheduleIterAwaitNext<V, S, T>) -> Cps<V, S, T> {
        Cps {
            state: State::Await(kont.state_await),
            env: kont.env,
        }
    }
}

impl<V, S, T> KontScheduleIterAwaitNext<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    pub fn proceed(self) -> Kont<V, S, T> {
        Cps::from(self).step()
    }
}

impl<V, S, T> From<KontAwaitScheduledNext<V, S, T>> for Cps<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    fn from(kont: KontAwaitScheduledNext<V, S, T>) -> Cps<V, S, T> {
        Cps {
            state: State::Await(kont.state_await),
            env: kont.env,
        }
    }
}

impl<V, S, T> KontAwaitScheduledNext<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    pub fn item_arrived(mut self, await_iter: S, item: T) -> Kont<V, S, T> {
        assert!(self.state_await.pending_count > 0);
        self.env.fronts.push(Front { front_item: item, iter: await_iter, });
        self.state_await.pending_count -= 1;
        Cps::from(self).step()
    }

    pub fn no_more(mut self) -> Kont<V, S, T> {
        assert!(self.state_await.pending_count > 0);
        self.state_await.pending_count -= 1;
        Cps::from(self).step()
    }
}

impl<V, S, T> From<KontEmitDeprecatedNext<V, S, T>> for Cps<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    fn from(kont: KontEmitDeprecatedNext<V, S, T>) -> Cps<V, S, T> {
        Cps {
            state: State::Flush,
            env: kont.env,
        }
    }
}

impl<V, S, T> KontEmitDeprecatedNext<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    pub fn proceed(self) -> Kont<V, S, T> {
        Cps::from(self).step()
    }
}

impl<V, S, T> From<KontEmitItemNext<V, S, T>> for Cps<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    fn from(kont: KontEmitItemNext<V, S, T>) -> Cps<V, S, T> {
        Cps {
            state: State::Await(StateAwait::default()),
            env: kont.env,
        }
    }
}

impl<V, S, T> KontEmitItemNext<V, S, T> where V: DerefMut<Target = Vec<S>>, T: ComparableItem {
    pub fn proceed(self) -> Kont<V, S, T> {
        Cps::from(self).step()
    }
}

impl<S, T> PartialEq for Front<S, T> where T: ComparableItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<S, T> Eq for Front<S, T> where T: ComparableItem { }

impl<S, T> Ord for Front<S, T> where T: ComparableItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.front_item
            .compare_primary(&other.front_item)
            .reverse()
            .then_with(|| {
                self.front_item
                    .compare_secondary(&other.front_item)
                    .reverse()
            })
    }
}

impl<S, T> PartialOrd for Front<S, T> where T: ComparableItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cmp::{
            Ordering,
        },
    };

    use crate::{
        iters_merger::{
            Kont,
            KontScheduleIterAwait,
            KontAwaitScheduled,
            KontEmitItem,
            KontEmitDeprecated,
            Cps,
            ComparableItem,
        },
    };

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    struct S(i32);

    impl ComparableItem for S {
        fn compare_primary(&self, other: &Self) -> Ordering {
            self.0.cmp(&other.0)
        }

        fn compare_secondary(&self, _other: &Self) -> Ordering {
            Ordering::Equal
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    struct P(u64, u64);

    impl ComparableItem for P {
        fn compare_primary(&self, other: &Self) -> Ordering {
            self.0.cmp(&other.0)
        }

        fn compare_secondary(&self, other: &Self) -> Ordering {
            self.1.cmp(&other.1)
        }
    }

    #[test]
    fn merge_basic() {
        let mut iters = vec![
            vec![S(4), S(8), S(9)],
            vec![S(3), S(6)],
            vec![S(0), S(1), S(2), S(5)],
            vec![],
            vec![S(7), S(10), S(11)],
        ];

        let iters_merger = Cps::new(&mut iters);
        let mut await_set = vec![];
        let mut kont = iters_merger.step();
        let mut output = vec![];
        loop {
            kont = match kont {
                Kont::ScheduleIterAwait(KontScheduleIterAwait {
                    await_iter,
                    next,
                }) => {
                    await_set.push(await_iter);
                    next.proceed()
                },
                Kont::AwaitScheduled(KontAwaitScheduled { next, }) => {
                    let mut await_iter = await_set.pop().unwrap();
                    if await_iter.is_empty() {
                        next.no_more()
                    } else {
                        let item = await_iter.remove(0);
                        next.item_arrived(await_iter, item)
                    }
                },
                Kont::EmitDeprecated(KontEmitDeprecated { .. }) => {
                    unreachable!();
                },
                Kont::EmitItem(KontEmitItem {
                    item,
                    next,
                }) => {
                    output.push(item);
                    next.proceed()
                },
                Kont::Finished =>
                    break,
            };
        }

        assert_eq!(output, vec![S(0), S(1), S(2), S(3), S(4), S(5), S(6), S(7), S(8), S(9), S(10), S(11)]);
    }

    #[test]
    fn merge_deprecated_different_iters() {
        let mut iters = vec![
            vec![P(0, 0), P(1, 3), P(2, 0)],
            vec![P(0, 1), P(1, 1), P(2, 1)],
            vec![P(1, 0), P(1, 2), P(1, 4), P(3, 0)],
            vec![],
            vec![P(0, 2)],
        ];
        let (output, deprecated) = perform_merge(&mut iters);

        assert_eq!(output, vec![P(0, 2), P(1, 4), P(2, 1), P(3, 0)]);
        assert_eq!(deprecated, vec![P(0, 0), P(0, 1), P(1, 0), P(1, 1), P(1, 2), P(1, 3), P(2, 0)]);
    }

    #[test]
    fn merge_deprecated_same_iter() {
        let mut iters = vec![
            vec![P(0, 0), P(2, 0)],
            vec![P(2, 1)],
            vec![P(1, 0), P(1, 1), P(1, 2), P(1, 3), P(1, 4)],
            vec![],
        ];
        let (output, deprecated) = perform_merge(&mut iters);

        assert_eq!(output, vec![P(0, 0), P(1, 4), P(2, 1)]);
        assert_eq!(deprecated, vec![P(1, 0), P(1, 1), P(1, 2), P(1, 3), P(2, 0)]);
    }

    #[test]
    fn stress_10_derived_00() {
        let mut iters = vec![
            vec![P(17556261672556393688, 1)],
            vec![P(8093639385199525262, 3)],
            vec![P(8093639385199525262, 0), P(16107956337732637054, 1)],
            vec![P(3144521344682756311, 7), P(3144521344682756311, 8)],
            vec![P(3144521344682756311, 0), P(16010932765423118261, 3)],
            vec![P(3144521344682756311, 6)],
            vec![P(16122783848902096046, 2)],
            vec![P(6956425666106740206, 5), P(17556261672556393688, 2)],
            vec![P(16748507097280151939, 0)],
            vec![P(6991660086521729989, 0)],
            vec![P(6956425666106740206, 1)],
            vec![P(3144521344682756311, 4), P(6956425666106740206, 4)],
            vec![P(8093639385199525262, 1), P(16010932765423118261, 0)],
            vec![P(11486697784006332595, 1), P(16010932765423118261, 4)],
            vec![P(6956425666106740206, 3), P(8093639385199525262, 4)],
            vec![P(3144521344682756311, 2), P(16010932765423118261, 5)],
            vec![P(3144521344682756311, 1)],
            vec![P(6991660086521729989, 2)],
            vec![P(16010932765423118261, 7)],
            vec![P(8093639385199525262, 6), P(16010932765423118261, 6)],
            vec![P(17556261672556393688, 0)],
            vec![P(16122783848902096046, 0)],
            vec![P(3144521344682756311, 5), P(6956425666106740206, 0)],
            vec![P(8093639385199525262, 5)],
            vec![P(3144521344682756311, 9)],
            vec![P(11486697784006332595, 0)],
            vec![P(16010932765423118261, 8)],
            vec![P(6956425666106740206, 2)],
            vec![P(3144521344682756311, 3), P(11486697784006332595, 2)],
            vec![P(16010932765423118261, 1)],
            vec![P(16107956337732637054, 0)],
            vec![P(8093639385199525262, 2), P(16010932765423118261, 2)],
            vec![P(11486697784006332595, 3)],
            vec![P(6991660086521729989, 1), P(16122783848902096046, 1)],
        ];
        let sample_output = vec![
            P(3144521344682756311, 9),
            P(6956425666106740206, 5),
            P(6991660086521729989, 2),
            P(8093639385199525262, 6),
            P(11486697784006332595, 3),
            P(16010932765423118261, 8),
            P(16107956337732637054, 1),
            P(16122783848902096046, 2),
            P(16748507097280151939, 0),
            P(17556261672556393688, 2),
        ];
        let sample_deprecated = vec![
            P(3144521344682756311, 0),
            P(3144521344682756311, 1),
            P(3144521344682756311, 2),
            P(3144521344682756311, 3),
            P(3144521344682756311, 4),
            P(3144521344682756311, 5),
            P(3144521344682756311, 6),
            P(3144521344682756311, 7),
            P(3144521344682756311, 8),
            P(6956425666106740206, 0),
            P(6956425666106740206, 1),
            P(6956425666106740206, 2),
            P(6956425666106740206, 3),
            P(6956425666106740206, 4),
            P(6991660086521729989, 0),
            P(6991660086521729989, 1),
            P(8093639385199525262, 0),
            P(8093639385199525262, 1),
            P(8093639385199525262, 2),
            P(8093639385199525262, 3),
            P(8093639385199525262, 4),
            P(8093639385199525262, 5),
            P(11486697784006332595, 0),
            P(11486697784006332595, 1),
            P(11486697784006332595, 2),
            P(16010932765423118261, 0),
            P(16010932765423118261, 1),
            P(16010932765423118261, 2),
            P(16010932765423118261, 3),
            P(16010932765423118261, 4),
            P(16010932765423118261, 5),
            P(16010932765423118261, 6),
            P(16010932765423118261, 7),
            P(16107956337732637054, 0),
            P(16122783848902096046, 0),
            P(16122783848902096046, 1),
            P(17556261672556393688, 0),
            P(17556261672556393688, 1),
        ];

        let (output, deprecated) = perform_merge(&mut iters);

        assert_eq!(output, sample_output);
        assert_eq!(deprecated, sample_deprecated);
    }

    #[test]
    fn stress() {
        use std::collections::{
            HashMap,
        };

        use rand::{
            Rng,
        };

        let total_items = 32764;
        let mut items = Vec::with_capacity(total_items);
        let mut rng = rand::thread_rng();
        for _ in 0 .. total_items {
            let item: u64 = rng.gen();
            let versions_count = rng.gen_range(1 ..= 10);
            for version in 0 .. versions_count {
                items.push(P(item, version));
            }
        }

        let mut sample_output_map = HashMap::new();
        let mut sample_deprecated = Vec::new();

        let mut iters = Vec::new();
        while !items.is_empty() {
            let lo = total_items / 100;
            let hi = total_items / 3;
            let iter_items_count = rng.gen_range(lo .. hi);
            let mut iter = Vec::with_capacity(iter_items_count);
            for _ in 0 .. iter_items_count {
                if items.is_empty() {
                    break;
                }
                let index = rng.gen_range(0 .. items.len());
                let item = items.swap_remove(index);
                iter.push(item);

                match sample_output_map.get(&item.0) {
                    None => {
                        sample_output_map.insert(item.0, item.1);
                    },
                    Some(&version) if version < item.1 => {
                        sample_deprecated.push(P(item.0, version));
                        sample_output_map.insert(item.0, item.1);
                    },
                    Some(..) => {
                        sample_deprecated.push(item);
                    },
                }
            }
            iter.sort();
            iters.push(iter);
        }

        let mut sample_output: Vec<_> = sample_output_map
            .into_iter()
            .map(|(k, v)| P(k, v))
            .collect();
        sample_output.sort();
        sample_deprecated.sort();

        let (output, deprecated) = perform_merge(&mut iters);

        assert_eq!(output, sample_output);
        assert_eq!(deprecated, sample_deprecated);
    }

    fn perform_merge(iters: &mut Vec<Vec<P>>) -> (Vec<P>, Vec<P>) {
        let iters_merger = Cps::new(iters);
        let mut await_set = vec![];
        let mut kont = iters_merger.step();
        let mut output = vec![];
        let mut deprecated = vec![];
        loop {
            kont = match kont {
                Kont::ScheduleIterAwait(KontScheduleIterAwait {
                    await_iter,
                    next,
                }) => {
                    await_set.push(await_iter);
                    next.proceed()
                },
                Kont::AwaitScheduled(KontAwaitScheduled { next, }) => {
                    let mut await_iter = await_set.pop().unwrap();
                    if await_iter.is_empty() {
                        next.no_more()
                    } else {
                        let item = await_iter.remove(0);
                        next.item_arrived(await_iter, item)
                    }
                },
                Kont::EmitDeprecated(KontEmitDeprecated {
                    item,
                    next,
                }) => {
                    deprecated.push(item);
                    next.proceed()
                },
                Kont::EmitItem(KontEmitItem {
                    item,
                    next,
                }) => {
                    output.push(item);
                    next.proceed()
                },
                Kont::Finished =>
                    break,
            };
        }

        deprecated.sort();
        (output, deprecated)
    }
}
