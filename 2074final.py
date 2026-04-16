import requests
import pandas as pd
import json
import re

# Input Reference Files
ADMIN_REF_PATH = ##input path to admin_ref.csv##
PARTY_REF_PATH = ## input path to parties_ref.csv##

# Output Paths
RAW_OUTPUT_PATH   = ##raw output localelections2074_raw.csv
FINAL_OUTPUT_PATH = ## clean output localelections2074_final.csv

POST_MAP = {
    "अध्यक्ष": "Mayor", "प्रमुख": "Mayor",
    "उपप्रमुख": "Deputy Mayor", "उपाध्यक्ष": "Deputy Mayor",
    "वडा अध्यक्ष": "Ward Chair", "महिला सदस्य": "Female Ward Member",
    "दलित महिला सदस्य": "Dalit Female Ward Member", "सदस्य": "Member"
}

GENDER_MAP = {"पुरुष": "Male", "महिला": "Female", "तेस्रो लिङ्गी": "Third Gender"}

def clean_text(text):
    if pd.isna(text): return None
    return re.sub(r'\s+', ' ', str(text)).strip()

# --- PART 1: Downloading raw data ---

print("=" * 60)
print("PART 1: Fetching 2074 Election Data from Election Commission...")
VOTE_URLS = [
    "https://result.election.gov.np/JSONFiles/VoteCount1.txt",
    "https://result.election.gov.np/JSONFiles/VoteCount2.txt",
    "https://result.election.gov.np/JSONFiles/VoteCount3.txt"
]

all_frames = []
for url in VOTE_URLS:
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        r.encoding = 'utf-8-sig'
        df_temp = pd.DataFrame(json.loads(r.text))
        all_frames.append(df_temp)
        print(f"Successfully downloaded: {url.split('/')[-1]}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")

df_raw = pd.concat(all_frames, ignore_index=True)
df_raw.columns = df_raw.columns.str.lower()
df_raw.to_csv(RAW_OUTPUT_PATH, index=False, encoding='utf-8-sig')

# --- PART 2: Mapping using reference files ---

print("\nPART 2: Mapping with Administrative and Party Reference Files...")

try:
    # 1. Load Reference Files
    df_admin = pd.read_csv(ADMIN_REF_PATH)
    df_party = pd.read_csv(PARTY_REF_PATH)

    # 2. Build Admin Lookup
    df_admin['dist_clean'] = df_admin['district_nepali'].astype(str).str.strip()
    df_admin['loc_clean'] = df_admin['local_2074'].astype(str).str.strip()
    
    admin_lookup = df_admin.set_index(['dist_clean', 'loc_clean'])[
        ['district_eng', 'local_unit_eng', 'local_unit_code', 'ecob', 'mntp', 'province']
    ].to_dict('index')

    # 3. Build Party Lookup
    party_lookup = dict(zip(
        df_party['nepali_2074'].astype(str).str.strip(),
        df_party['english_master']
    ))

    # 4. Process Raw Data
    df_raw['dist_np_clean'] = df_raw['districtname'].astype(str).str.strip()
    df_raw['loc_np_clean'] = df_raw['localbodyname'].astype(str).str.strip()

    def map_admin(row):
        key = (row['dist_np_clean'], row['loc_np_clean'])
        res = admin_lookup.get(key, {})
        return pd.Series([
            res.get('district_eng'), res.get('local_unit_eng'), 
            res.get('local_unit_code'), res.get('ecob'), 
            res.get('mntp'), res.get('province')
        ])

    df_raw[['district_en', 'local_unit_en', 'local_unit_code', 'ecob', 'mntp', 'province_en']] = df_raw.apply(map_admin, axis=1)

    # Apply Party, Post, Gender, and Rank
    df_raw['party_en'] = df_raw['politicalpartyname'].astype(str).str.strip().map(party_lookup)
    df_raw['post_en'] = df_raw['postname'].apply(clean_text).map(POST_MAP)
    df_raw['gender_en'] = df_raw['gender'].apply(clean_text).map(GENDER_MAP)
    df_raw['elected_flag'] = (df_raw['estatus'].astype(str).str.strip() == 'E').astype(int)

    # 5. Ranking
    df_raw['rank'] = (
        df_raw.groupby(['district_en', 'local_unit_en', 'wardno', 'post_en'], dropna=False)['totalvotesrecieved']
        .rank(method='min', ascending=False)
        .astype('Int64')
    )

    # 6. Final Column Organization
    final_cols = {
        'province_en': 'province',
        'district_en': 'district',
        'local_unit_en': 'local_unit',
        'local_unit_code': 'local_unit_code',
        'ecob': 'ecob',
        'mntp': 'mntp',
        'wardno': 'wardno',
        'post_en': 'post',
        'candidatename': 'candidate_name',
        'age': 'age', 
        'gender' : 'gender_np',
        'gender_en': 'gender',
        'party_en': 'party_master',
        'totalvotesrecieved': 'votes',
        'rank': 'rank',
        'elected_flag': 'elected'
    }
    
    # Ensure age is numeric
    if 'age' in df_raw.columns:
        df_raw['age'] = pd.to_numeric(df_raw['age'], errors='coerce')

    # Select and rename columns
    existing_cols = [c for c in final_cols.keys() if c in df_raw.columns]
    df_final = df_raw.rename(columns=final_cols)[ [final_cols[k] for k in existing_cols] ]
    
    # Drop the temporary nepali gender column if it was included
    if 'gender_np' in df_final.columns:
        df_final = df_final.drop(columns=['gender_np'])

    # 7. Coverage Audit Summary
    total = len(df_final)
    admin_success = df_final['local_unit'].notna().sum()
    party_success = df_final['party_master'].notna().sum()

    print("-" * 60)
    print(f"MAPPING COVERAGE REPORT")
    print("-" * 60)
    print(f"Administrative Mapping: {admin_success}/{total} ({ (admin_success/total)*100:.2f}%)")
    print(f"Political Party Mapping: {party_success}/{total} ({ (party_success/total)*100:.2f}%)")

    # Final Save
    df_final.to_csv(FINAL_OUTPUT_PATH, index=False, encoding='utf-8-sig')
    print("-" * 60)
    print(f"[SUCCESS] Final analysis-ready file saved to: {FINAL_OUTPUT_PATH}")

except Exception as e:
    print(f"CRITICAL ERROR: {e}")
