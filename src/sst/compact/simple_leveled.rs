use crate::sst::Sstables;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

// async fn compact<File>(snapshot: &Sstables<File>, options: SimpleLeveledCompactionOptions) {
//     (0..snapshot.levels().len() + 1)
//         .tuple_windows()
//         .find_map(|(upper_level, lower_level)| {
//             let upper = snapshot.table_ids(upper_level).to_vec();
//             if upper_level == 0 && upper.len() < options.level0_file_num_compaction_trigger
//             {
//                 None
//             } else {
//                 let lower = snapshot.table_ids(lower_level).to_vec();
//                 let ratio = (lower.len() as f32) / (upper.len() as f32);
//                 if ratio * 100.0 < options.size_ratio_percent as f32 {
//                     let task = SimpleLeveledCompactionTask {
//                         upper_level,
//                         upper_level_sst_ids: upper.clone(),
//                         lower_level,
//                         lower_level_sst_ids: lower.clone(),
//                         is_lower_level_bottom_level: lower_level == snapshot.levels().len(),
//                     };
//                     println!(
//                         "compaction triggered at level {} and {} with size ratio {}",
//                         upper_level, lower_level, ratio
//                     );
//                     Some(task)
//                 } else {
//                     None
//                 }
//             }
//         });
// }
