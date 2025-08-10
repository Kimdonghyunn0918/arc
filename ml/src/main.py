from ml.src import data_loader, model
from ml import config

def run_analysis():
    """전체 분석 파이프라인을 실행합니다."""
    print("Starting ML analysis pipeline...")
    
    # 1. Elasticsearch 클라이언트 생성
    es_client = data_loader.get_es_client()
    if not es_client:
        return

    # 2. 데이터 가져오기
    df = data_loader.fetch_data(es_client, config.SOURCE_INDEX_PATTERN)
    if df.empty:
        print("No data fetched. Exiting.")
        return

    # 3. 데이터 전처리
    df_processed = model.preprocess_data(df)

    # 4. 모델 훈련 및 예측
    anomalies = model.train_and_predict(df_processed)
    
    # 5. 결과 처리
    df['is_anomaly'] = anomalies
    anomalous_data = df[df['is_anomaly'] == -1]
    
    print(f"\nAnalysis complete. Found {len(anomalous_data)} potential anomalies.")
    
    if not anomalous_data.empty:
        print("Sample of anomalous data:")
        # 필요한 정보만 출력
        print(anomalous_data[['@timestamp', 'source.ip', 'destination.ip', 'event.kind', 'suricata.eve.alert.signature']].head())

    # TODO: 분석 결과를 새로운 Elasticsearch 인덱스에 저장하는 로직 추가

if __name__ == "__main__":
    run_analysis()
