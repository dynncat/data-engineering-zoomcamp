import pandas as pd
from sqlalchemy import create_engine
import io

def main():
    # 1. DB 연결 설정 (아까 포트를 5433으로 바꿨다면 5433으로 쓰세요!)
    engine = create_engine('postgresql://root:root@localhost:5433/ny_taxi')

    # 2. 데이터 URL 설정
    taxi_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
    zone_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    print("URL에서 데이터를 직접 읽어오는 중...")

    # 3. 택시 데이터 읽기 (Parquet)
    df_taxi = pd.read_parquet(taxi_url)
    
    # 존 데이터 읽기 (CSV)
    df_zones = pd.read_csv(zone_url)

    print(f"데이터 로드 완료! (택시 데이터: {len(df_taxi)} 행)")

    # 4. DB에 저장
    print("DB에 데이터를 기록하는 중...")
    df_taxi.to_sql(name='green_taxi_data', con=engine, if_exists='replace', index=False)
    df_zones.to_sql(name='zones', con=engine, if_exists='replace', index=False)

    print("모든 데이터가 성공적으로 저장되었습니다! 🚀")

if __name__ == "__main__":
    main()