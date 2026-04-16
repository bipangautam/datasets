import requests
import pandas as pd
import json
import re
import time


# Input Reference Files
ADMIN_REF_PATH = ##input path to admin_ref.csv##
PARTY_REF_PATH = ## input path to parties_ref.csv##

# Output Paths
RAW_OUTPUT_PATH   = ## output path to localelection2079_raw.csv"
FINAL_OUTPUT_PATH = ## output path to clean localelection2079_final.csv"


POST_MAP = {
    1: "Mayor", 2: "Deputy Mayor",
    3: "Mayor", 4: "Deputy Mayor",
    5: "Ward Chair", 6: "Member",
    7: "Female Ward Member", 8: "Dalit Female Ward Member"
}

GENDER_MAP = {"पुरुष": "Male", "महिला": "Female", "तेस्रो लिङ्गी": "Third Gender"}


def clean_text(text):
    if pd.isna(text): return None
    return re.sub(r'\s+', ' ', str(text)).strip()

headers = {"User-Agent": "Mozilla/5.0"}

def download_json(url):
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    r.encoding = 'utf-8-sig'
    df = pd.DataFrame(json.loads(r.text))
    df.columns = df.columns.str.lower()
    return df

## 1. Crosswalk for downloading 

print("=" * 60)
print("PART 1: Using crosswalk from election commission")
BASE_URL = "https://result.election.gov.np/JSONFiles/Election2079/Local/Lookup"

try:
    df_district = download_json(f"{BASE_URL}/districts.json")
    df_district = df_district[df_district['name'] != 'NA'].copy() 
    
    df_lu_raw = download_json(f"{BASE_URL}/localbodies.json")

    # Link District -> Local Unit
    df_lu_meta = df_lu_raw.merge(df_district[['id', 'name']], left_on='parentid', right_on='id', suffixes=('', '_dist'))
    
    df_lu_meta['dist_np_clean'] = df_lu_meta['name_dist'].astype(str).str.strip()
    df_lu_meta['loc_np_clean'] = df_lu_meta['name'].astype(str).str.strip()

## 2. Mapping admin with admin_ref

    print("PART 2: Mapping Administrative Data from Reference...")
    df_admin = pd.read_csv(ADMIN_REF_PATH)
    
    # Prepare mapping keys
    df_admin['dist_clean'] = df_admin['district_nepali'].astype(str).str.strip()
    df_admin['loc_clean'] = df_admin['local_2079'].astype(str).str.strip()
    
    # Create lookup dict including the English 'province' column from admin_ref
    admin_lookup = df_admin.set_index(['dist_clean', 'loc_clean'])[
        ['district_eng', 'local_unit_eng', 'local_unit_code', 'ecob', 'mntp', 'province']
    ].to_dict('index')

    def apply_admin_map(row):
        key = (row['dist_np_clean'], row['loc_np_clean'])
        res = admin_lookup.get(key, {})
        return pd.Series([
            res.get('district_eng'), res.get('local_unit_eng'), 
            res.get('local_unit_code'), res.get('ecob'), res.get('mntp'),
            res.get('province') # This pulls the English Province from admin_ref
        ])

    df_lu_meta[['dist_en', 'loc_en', 'loc_code', 'ecob', 'mntp', 'province_en']] = df_lu_meta.apply(apply_admin_map, axis=1)

## 3. Scraping

    print("PART 3: Scraping Vote Data...")
    VOTE_URL = "https://result.election.gov.np/JSONFiles/Election2079/Local/{}.json"
    all_data = []
    
    lb_ids = df_lu_meta['id'].unique()
    for i, lb_id in enumerate(lb_ids):
        try:
            r = requests.get(VOTE_URL.format(lb_id), headers=headers, timeout=15)
            if r.status_code == 200:
                r.encoding = 'utf-8-sig'
                data = json.loads(r.text)
                if isinstance(data, list):
                    for entry in data:
                        entry['lu_id_ref'] = int(lb_id)
                        all_data.append(entry)
            if (i+1) % 100 == 0: print(f"Progress: {i+1}/{len(lb_ids)} units scraped...")
            time.sleep(0.05)
        except: continue

    df_raw = pd.DataFrame(all_data)
    df_raw.columns = df_raw.columns.str.lower()
    
    # Filter Post IDs 9-12
    EXCLUDED_POST_IDS = [9, 10, 11, 12]
    df_vote = df_raw[~df_raw['postid'].isin(EXCLUDED_POST_IDS)].copy()
    
    # Merge Scraped Data with Geographic Metadata
    df_vote = df_vote.merge(df_lu_meta, left_on='lu_id_ref', right_on='id', how='left')
    df_vote.to_csv(RAW_OUTPUT_PATH, index=False, encoding='utf-8-sig')

    # Party Mapping
    df_party_ref = pd.read_csv(PARTY_REF_PATH)
    party_lookup = dict(zip(df_party_ref['nepali_2079'].astype(str).str.strip(), df_party_ref['english_master']))

    # Final Transformations
    df_vote['party_master'] = df_vote['politicalpartyname'].astype(str).str.strip().map(party_lookup)
    df_vote['post_en'] = pd.to_numeric(df_vote['postid'], errors='coerce').map(POST_MAP)
    df_vote['gender_en'] = df_vote['gender'].astype(str).str.strip().map(GENDER_MAP)
    df_vote['elected'] = df_vote['remarkseng'].astype(str).str.strip().str.lower().eq('elected').astype(int)

    # Ranking
    df_vote['rank'] = (
        df_vote.groupby(['dist_en', 'loc_en', 'ward', 'post_en'], dropna=False)['totalvotereceived']
        .rank(method='min', ascending=False).astype('Int64')
    )

    # Final Column Selection 
    final_cols = {
        'province_en': 'province', 
        'dist_en': 'district', 
        'loc_en': 'local_unit',
        'loc_code': 'local_unit_code', 
        'ecob': 'ecob', 
        'mntp': 'mntp',
        'ward': 'wardno', 
        'post_en': 'post', 
        'candidatenameeng': 'candidate_name',
        'age': 'age',               # Added Age
        'gender' : 'gender_np',     # Added gender_np mapping
        'gender_en': 'gender', 
        'party_master': 'party_master',
        'totalvotereceived': 'votes', 
        'rank': 'rank', 
        'elected': 'elected'
    }
    
    # Renaming and selecting
    df_final = df_vote.rename(columns=final_cols)
    
    # Filter to only the target column names specified in final_cols values
    df_final = df_final[list(final_cols.values())]

    # Drop gender_np if it exists (Cleaning the temporary column)
    if 'gender_np' in df_final.columns:
        df_final = df_final.drop(columns=['gender_np'])

    # Audit & Save
    print("-" * 60)
    print(f"Admin Coverage: {df_final['local_unit'].notna().sum()}/{len(df_final)} ({(df_final['local_unit'].notna().sum()/len(df_final))*100:.2f}%)")
    print(f"Party Coverage: {df_final['party_master'].notna().sum()}/{len(df_final)} ({(df_final['party_master'].notna().sum()/len(df_final))*100:.2f}%)")

    df_final.to_csv(FINAL_OUTPUT_PATH, index=False, encoding='utf-8-sig')
    print("-" * 60)
    print(f"[SUCCESS] Final file saved to: {FINAL_OUTPUT_PATH}")

except Exception as e:
    print(f"ERROR: {e}")
