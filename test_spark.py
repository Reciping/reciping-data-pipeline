# test_spark.py (최종 수정본)
from pyspark.sql import SparkSession

def main():
    """가장 안정적인 설정으로 스파크를 테스트하는 함수"""
    try:
        # 1. 어떤 환경에서도 실행되도록 보장하는 설정 추가
        #    이 두 줄의 코드가 모든 네트워크 및 방화벽 문제를 해결합니다.
        spark = SparkSession.builder \
            .appName("PoetrySparkFinalTest") \
            .master("local[*]") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.ui.enabled", "false") \
            .getOrCreate()

        print("\n✅✅✅ SparkSession이 성공적으로 생성되었습니다! ✅✅✅")
        print(f"✅ Spark 버전: {spark.version}\n")

        # 2. 간단한 데이터프레임 생성 및 출력
        data = [("성공", 100)]
        columns = ["결과", "점수"]
        df = spark.createDataFrame(data, columns)

        print("테스트 DataFrame:")
        df.show()

        # 3. SparkSession 종료
        spark.stop()
        print("\n✅ SparkSession이 성공적으로 종료되었습니다.")

    except Exception as e:
        print("\n❌❌❌ 스파크 실행 중 에러가 발생했습니다. ❌❌❌")
        print(e)


if __name__ == "__main__":
    main()