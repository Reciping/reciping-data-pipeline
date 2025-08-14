# 📁 프로젝트 파일 정리 계획

## 🎯 정리 목표
- 중복되거나 사용하지 않는 파일 제거
- 최종 프로덕션 코드와 문서만 유지
- 명확한 디렉토리 구조 구성

---

## 🔍 현재 상태 분석

### 🐍 Python 파일들 (19개)

#### ✅ **보존할 핵심 파일**
1. `bronze_to_silver_iceberg.py` - **Silver Layer 변환 (프로덕션)**
2. `compatible_kst_fact_processor.py` - **Gold Layer KST 최적화 처리 (프로덕션)**
3. `upload_to_landing_zone.py` - **S3 업로드 유틸리티**
4. `check_conversion_results.py` - **데이터 검증 도구**
5. `iceberg_table_maintenance.py` - **Iceberg 테이블 관리**
6. `streaming_to_iceberg.py` - **스트리밍 처리**

#### ❌ **제거할 중복/테스트 파일 (13개)**
1. `gold_layer_star_schema.py` - ❌ **JVM 크래시 문제, 사용하지 않음**
2. `ultra_batch_processor.py` - ❌ **초기 테스트 버전**
3. `smart_batch_processor.py` - ❌ **실험 버전**
4. `improved_batch_processor.py` - ❌ **실험 버전**
5. `kst_optimized_fact_processor.py` - ❌ **compatible 버전으로 대체됨**
6. `date_range_gold_processor.py` - ❌ **호환성 문제**
7. `gold_layer_complete.py` - ❌ **사용하지 않음**
8. `gold_layer_practical.py` - ❌ **사용하지 않음**
9. `gold_layer_minimal.py` - ❌ **사용하지 않음**
10. `gold_layer_safe.py` - ❌ **사용하지 않음**
11. `gold_layer_analytics.py` - ❌ **사용하지 않음**
12. `bronze_to_silver_simple.py` - ❌ **Iceberg 버전으로 대체됨**
13. `bronze_to_silver_final.py` - ❌ **Iceberg 버전으로 대체됨**

### 📄 Markdown 문서들 (8개)

#### ✅ **보존할 최종 문서**
1. `COMPLETE_LAKEHOUSE_DOCUMENTATION.md` - **💎 최종 완전 가이드 (방금 생성)**
2. `README.md` - **프로젝트 기본 정보**

#### ❌ **제거할 중복 문서 (6개)**
1. `COMPLETE_PROJECT_DOCUMENTATION.md` - ❌ **COMPLETE_LAKEHOUSE로 대체됨**
2. `ICEBERG_ETL_IMPLEMENTATION_SUMMARY.md` - ❌ **최종 문서에 포함됨**
3. `S3_DATA_LAKEHOUSE_ARCHITECTURE.md` - ❌ **최종 문서에 포함됨**
4. `ADVANCED_FEATURES_SUMMARY.md` - ❌ **최종 문서에 포함됨**
5. `GOLD_LAYER_EXECUTION_GUIDE.md` - ❌ **최종 문서에 포함됨**
6. `GOLD_LAYER_METRICS_IMPLEMENTATION_GUIDE.md` - ❌ **최종 문서에 포함됨**

---

## 🗂️ 제안하는 새로운 구조

```
reciping-data-pipeline/
├── 📁 core/                              # 핵심 파이프라인
│   ├── bronze_to_silver_iceberg.py       # Silver Layer 변환
│   ├── compatible_kst_fact_processor.py  # Gold Layer KST 처리
│   └── iceberg_table_maintenance.py      # 테이블 관리
│
├── 📁 utils/                             # 유틸리티
│   ├── upload_to_landing_zone.py         # S3 업로드
│   ├── check_conversion_results.py       # 데이터 검증
│   └── streaming_to_iceberg.py           # 스트리밍
│
├── 📁 docs/                              # 문서
│   ├── COMPLETE_LAKEHOUSE_DOCUMENTATION.md  # 💎 완전 가이드
│   └── README.md                         # 프로젝트 정보
│
├── 📁 config/                            # 설정
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── requirements.txt
│   └── pyproject.toml
│
├── 📁 data/                              # 데이터 (유지)
├── 📁 notebooks/                         # Jupyter 노트북
│   ├── create_data.ipynb
│   ├── create_log_data.ipynb
│   └── read_event_logs.ipynb
│
└── 📁 archive/                           # 기존 아카이브 (유지)
```

---

## 🛠️ 정리 작업 단계

### 1단계: 핵심 파일 이동
```bash
mkdir -p core utils docs config notebooks
```

### 2단계: 중복 파일 제거 (13개 Python + 6개 Markdown)
```bash
# Python 파일 제거
rm gold_layer_star_schema.py
rm ultra_batch_processor.py
rm smart_batch_processor.py
... (총 13개)

# Markdown 파일 제거  
rm COMPLETE_PROJECT_DOCUMENTATION.md
rm ICEBERG_ETL_IMPLEMENTATION_SUMMARY.md
... (총 6개)
```

### 3단계: 파일 재배치
```bash
# 핵심 파이프라인
mv bronze_to_silver_iceberg.py core/
mv compatible_kst_fact_processor.py core/
mv iceberg_table_maintenance.py core/

# 유틸리티
mv upload_to_landing_zone.py utils/
mv check_conversion_results.py utils/
mv streaming_to_iceberg.py utils/

# 문서
mv COMPLETE_LAKEHOUSE_DOCUMENTATION.md docs/

# 설정
mv docker-compose.yml config/
mv requirements.txt config/
mv pyproject.toml config/

# 노트북
mv *.ipynb notebooks/
```

---

## 📊 정리 효과

### 제거되는 파일
- **Python**: 13개 → **75% 감소**
- **Markdown**: 6개 → **75% 감소**
- **총 용량**: 약 80% 감소

### 남는 핵심 파일
- **Python**: 6개 (핵심 파이프라인)
- **Markdown**: 2개 (완전 가이드 + README)
- **구조**: 명확한 기능별 분류

---

## ⚡ 즉시 실행 가능

이 계획에 동의하시면 바로 정리 작업을 시작하겠습니다:

1. **안전한 삭제**: archive/ 폴더로 이동 후 삭제
2. **구조 재편**: 새로운 디렉토리 구조 생성
3. **문서 업데이트**: README.md 업데이트
4. **검증**: 핵심 기능 동작 확인

**진행하시겠습니까?** 🚀
