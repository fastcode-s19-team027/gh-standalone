use std::fs;
use std::io::{Result, Read, Write, BufWriter};
use std::collections::{HashSet, HashMap};
use std::iter::FromIterator;

#[macro_use]
extern crate serde;

const MIN_USER_RATING: u64 = 10;
const MAX_REL_REPO: usize = 100;

#[derive(Serialize, Deserialize, Debug)]
struct InputItem {
    #[serde(rename = "type")]
    event_type: String,
    user: String,
    repo: String,
}

fn load_data() -> Result<Vec<InputItem>> {
    let mut items = vec![];
    for path in fs::read_dir("./data/")? {
        let mut file = fs::File::open(path?.path())?;
        let mut s = "".to_owned();
        let _ = file.read_to_string(&mut s)?;
        items.extend(
            s.split_ascii_whitespace()
                .filter(|l| !l.is_empty())
                .map(|l| serde_json::from_str(&l).unwrap()))
    }
    Ok(items)
}

fn gen_uid_list(data: &[InputItem]) -> Vec<String> {
    let mut uids = HashSet::new();
    for d in data {
        uids.insert(d.user.to_owned());
    }
    Vec::from_iter(uids)
}

fn gen_rid_list(data: &[InputItem]) -> Vec<String> {
    let mut rids = HashSet::new();
    for d in data {
        rids.insert(d.repo.to_owned());
    }
    Vec::from_iter(rids)
}

fn compute_score(event_type: &str) -> u64 {
    match event_type {
        "WatchEvent" => 10,
        "ForkEvent" => 6,
        "IssuesEvent" => 1,
        "PullRequestEvent" => 2,
        _ => panic!(),
    }
}

fn list_to_map(list: &[String]) -> HashMap<&String, u32> {
    HashMap::from_iter(list.iter().enumerate().map(|(v, k)| (k, v as u32)))
}

fn calc_score(data: &[InputItem], uid_map: &HashMap<&String, u32>, rid_map: &HashMap<&String, u32>) -> Vec<Vec<(u32, u64)>> {
    let mut user_repo_score = vec![HashMap::new(); uid_map.len()];
    for d in data {
        let uid = *uid_map.get(&d.user).unwrap();
        let rid = *rid_map.get(&d.repo).unwrap();
        *user_repo_score[uid as usize].entry(rid).or_insert(0u64) += compute_score(&d.event_type);
    }
    user_repo_score.into_iter().map(|hm| hm.into_iter().filter(|(_, score)| *score >= MIN_USER_RATING).collect()).collect()
}

fn calc_score_repo(data: &[InputItem], uid_map: &HashMap<&String, u32>, rid_map: &HashMap<&String, u32>) -> Vec<Vec<(u32, u64)>> {
    let mut repo_user_score = vec![HashMap::new(); rid_map.len()];
    for d in data {
        let uid = *uid_map.get(&d.user).unwrap();
        let rid = *rid_map.get(&d.repo).unwrap();
        *repo_user_score[rid as usize].entry(uid).or_insert(0u64) += compute_score(&d.event_type);
    }
    repo_user_score.into_iter().map(|hm| hm.into_iter().filter(|(_, score)| *score >= MIN_USER_RATING).collect()).collect()
}

fn calc_repo_repo(user_repo_score: &Vec<Vec<(u32, u64)>>, repo_user_score: &Vec<Vec<(u32, u64)>>) -> Vec<Vec<(u32, u64)>> {
    let mut final_score = vec![vec![]; repo_user_score.len()];
    for (rid1, user_score) in repo_user_score.iter().enumerate() {
        let mut fscore = HashMap::new();
        for &(uid, score1) in user_score {
            for &(rid2, score2) in &user_repo_score[uid as usize] {
                *fscore.entry(rid2).or_insert(0u64) += score1 * score2;
            }
        }
        let mut fscore_vec = Vec::from_iter(fscore);
        fscore_vec.sort_by_key(|&(_, s)| std::cmp::Reverse(s));
        final_score[rid1].append(&mut fscore_vec.into_iter().take(MAX_REL_REPO).collect());
    }
    final_score
}

fn calc_final(user_repo_score: &Vec<Vec<(u32, u64)>>, repo_repo_score: &Vec<Vec<(u32, u64)>>) -> Vec<Vec<(u32, u64)>> {
    let mut final_score = vec![vec![]; user_repo_score.len()];
    for (uid, repo_score) in user_repo_score.iter().enumerate() {
        let mut fscore = HashMap::new();
        for &(rid_mid, score1) in repo_score {
            for &(rid, score2) in &repo_repo_score[rid_mid as usize] {
                *fscore.entry(rid).or_insert(0u64) += score1 * score2;
            }
        }
        let mut fscore_vec = Vec::from_iter(fscore);
        fscore_vec.sort_by_key(|&(_, s)| std::cmp::Reverse(s));
        final_score[uid].append(&mut fscore_vec.into_iter().take(MAX_REL_REPO).collect());
    }
    final_score
}

fn write_output(final_score: &Vec<Vec<(u32, u64)>>, uid_list: &[String], rid_list: &[String]) -> Result<()> {
    let mut file = BufWriter::new(fs::File::create("output")?);
    for (uid, repo_score) in final_score.iter().enumerate() {
        file.write_fmt(format_args!("{}\t{:?}\n", uid_list[uid as usize], repo_score.iter().map(|&(rid, score)| (&rid_list[rid as usize], score)).collect::<Vec<_>>()))?;
    }
    Ok(())
}

fn main() {
    println!("load data");
    let data = load_data().unwrap();
    let uid_list = gen_uid_list(&data);
    let rid_list = gen_rid_list(&data);
    let uid_map = list_to_map(&uid_list);
    let rid_map = list_to_map(&rid_list);
    println!("gen user => [(repo, score)]");
    let user_repo_score = calc_score(&data, &uid_map, &rid_map);
    println!("gen repo => [(user, score)]");
    let repo_user_score = calc_score_repo(&data, &uid_map, &rid_map);
    drop(uid_map);
    drop(rid_map);
    drop(data);
    println!("gen repo => [(repo, score)]");
    let repo_repo_score = calc_repo_repo(&user_repo_score, &repo_user_score);
    println!("gen final user => [(repo, score)]");
    let final_score = calc_final(&user_repo_score, &repo_repo_score);
    println!("output");
    write_output(&final_score, &uid_list, &rid_list).unwrap();
}
