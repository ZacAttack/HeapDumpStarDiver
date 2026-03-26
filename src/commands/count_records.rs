// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

use std::collections::HashMap;
use jvm_hprof::{EnumIterable, Hprof, RecordTag};

pub fn count_records(hprof: &Hprof) {
    let mut counts = RecordTag::iter()
        .map(|r| (r, 0_u64))
        .collect::<HashMap<RecordTag, u64>>();

    hprof
        .records_iter()
        .map(|r| r.unwrap().tag())
        .for_each(|tag| {
            counts.entry(tag).and_modify(|c| *c += 1).or_insert(1);
        });

    let mut counts: Vec<(RecordTag, u64)> = counts
        .into_iter()
        .collect::<Vec<(RecordTag, u64)>>();

    counts.sort_unstable_by_key(|&(_, count)| count);
    counts.reverse();

    for (tag, count) in counts {
        println!("{:?}: {}", tag, count);
    }
}
