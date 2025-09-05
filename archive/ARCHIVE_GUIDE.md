# 📁 Archive 폴더 완전 가이드

## 📋 목차
- [1. Archive 개요](#1-archive-개요)
- [2. 디렉토리 구조](#2-디렉토리-구조)
- [3. Legacy Files 상세 설명](#3-legacy-files-상세-설명)
- [4. 기존 Archive 구조](#4-기존-archive-구조)
- [5. 파일 복원 방법](#5-파일-복원-방법)
- [6. 정리 이유 요약](#6-정리-이유-요약)

---

## 1. Archive 개요

이 디렉토리는 **reciping-data-pipeline** 프로젝트의 모든 레거시, 실험, 중복 파일들을 체계적으로 보관하는 곳입니다. 프로덕션 환경에서는 사용하지 않지만, 개발 과정에서의 학습 자료나 문제 해결 참고용으로 보존되어 있습니다.

### 🎯 Archive 목적
- **코드 히스토리 보존**: 개발 과정에서의 시행착오와 학습 과정 기록
- **문제 해결 참고**: JVM 크래시 등의 문제 상황 재현 및 분석
- **프로젝트 정리**: 핵심 파일만 유지하여 유지보수성 향상
- **지식 공유**: 실험 과정과 결과를 팀원들과 공유

---

## 2. 디렉토리 구조

```
archive/
├── 📁 legacy_files/                    # 🆕 2025-08-14 정리된 파일들
│   ├── 🐍 python_experimental/         # 실험/중복 Python 파일 (16개)
│   ├── 📄 markdown_deprecated/         # 중복 문서 파일 (7개)
│   └── 📋 log_files/                  # JVM 에러 로그 (22개)
│
├── 📁 documentation/                   # 기존 문서 백업
├── 📁 failed_iceberg_attempts/         # Iceberg 실패 시도들
├── 📁 old_versions/                    # 이전 버전 파일들
└── 📁 test_files/                      # 테스트 파일들
```

---

## 3. Legacy Files 상세 설명

### 🐍 Python Experimental Files (16개)

#### **JVM 크래시 문제 파일**
##### `gold_layer_star_schema.py`
- **원인**: 복잡한 LEFT JOIN 연산으로 메모리 폭발
- **문제**: JVM SIGSEGV 크래시 빈발 (4GB 메모리로도 실패)
- **에러**: `# A fatal error has been detected by the Java Runtime Environment: SIGSEGV`
- **학습**: JOIN 기반 접근의 한계를 보여주는 중요한 사례

#### **실험 버전 배치 처리기들**
##### `ultra_batch_processor.py`
- **용도**: 초기 성공 버전 (5,000개 배치 크기 검증)
- **특징**: JOIN 없는 Denormalization 방식 도입
- **결과**: ✅ 성공 (메모리 안전성 확보)
- **대체**: `compatible_kst_fact_processor.py`로 KST 최적화 버전 개발

##### `smart_batch_processor.py`
- **용도**: 지능형 배치 크기 조절 실험
- **특징**: 동적 배치 크기 조정 알고리즘
- **결과**: ⚠️ 복잡성 증가로 안정성 저하
- **학습**: 단순한 고정 배치 크기가 더 안정적

##### `improved_batch_processor.py`
- **용도**: 성능 개선 시도
- **특징**: 다양한 최적화 기법 적용
- **결과**: ❌ 메모리 이슈 지속
- **학습**: 근본적 해결책(JOIN 제거) 필요성 확인

##### `kst_optimized_fact_processor.py`
- **용도**: KST 시간대 최적화 초기 버전
- **특징**: 한국 시간대 기반 분석 지원
- **결과**: ✅ 기능 성공, 안정성 부족
- **대체**: `compatible_kst_fact_processor.py`로 안정성 개선

##### `date_range_gold_processor.py`
- **용도**: 날짜 범위별 분할 처리 시도
- **특징**: 7/1~7/10, 7/11~7/20, 7/21~7/31 분할
- **결과**: ❌ Iceberg 스키마 호환성 문제
- **에러**: `IncompatibleClassChangeError: org.apache.iceberg.spark.SparkSchemaUtil`

#### **Gold Layer 변형들 (6개)**
##### `gold_layer_complete.py`, `gold_layer_practical.py`, `gold_layer_minimal.py`, `gold_layer_safe.py`, `gold_layer_analytics.py`
- **공통 목적**: Gold Layer 구현의 다양한 접근법 실험
- **차이점**: 
  - `complete`: 전체 기능 구현 시도
  - `practical`: 실용적 접근법
  - `minimal`: 최소 기능 구현
  - `safe`: 안전성 중심 설계
  - `analytics`: 분석 기능 특화
- **결과**: 모두 `compatible_kst_fact_processor.py`로 통합됨

#### **Bronze Layer 대체된 버전들**
##### `bronze_to_silver_simple.py`, `bronze_to_silver_final.py`
- **용도**: Bronze → Silver 변환의 초기 버전들
- **특징**: Parquet 형식 기반 (Iceberg 이전)
- **대체**: `bronze_to_silver_iceberg.py`로 Iceberg 지원 추가
- **학습**: 단순한 Parquet vs 고급 Iceberg의 차이

#### **빈 파일들 (2개)**
##### `streaming_to_iceberg.py`, `iceberg_table_maintenance.py`
- **상태**: 빈 파일 (코드 없음)
- **원인**: 기능 구현 예정이었으나 우선순위 변경
- **보존 이유**: 향후 스트리밍/유지보수 기능 개발 시 참고

#### **미실행 파일**
##### `check_conversion_results.py`
- **용도**: Conversion Rate 메트릭 결과 확인
- **상태**: 코드 작성됨, 실행하지 않음
- **이유**: Gold Layer 처리가 16.1%만 완료되어 메트릭 분석 시기상조
- **기능**: Iceberg 테이블에서 변환율 통계 추출

### 📄 Markdown Deprecated Files (7개)

#### **통합된 문서들**
##### `COMPLETE_PROJECT_DOCUMENTATION.md`
- **내용**: 프로젝트 전체 문서 (이전 버전)
- **대체**: `COMPLETE_LAKEHOUSE_DOCUMENTATION.md`로 완전 재작성
- **차이점**: KST 최적화, JVM 크래시 해결 과정 미포함

##### `ICEBERG_ETL_IMPLEMENTATION_SUMMARY.md`
- **내용**: Iceberg ETL 구현 요약
- **특징**: Bronze → Silver 변환 중심
- **통합**: 새 문서의 "Iceberg + Hive Metastore 구축" 섹션에 포함

##### `S3_DATA_LAKEHOUSE_ARCHITECTURE.md`
- **내용**: S3 데이터 레이크하우스 아키텍처 설명
- **특징**: 인프라 설계 중심
- **통합**: 새 문서의 "아키텍처 설계" 섹션에 포함

##### `ADVANCED_FEATURES_SUMMARY.md`
- **내용**: 고급 기능 요약
- **특징**: Iceberg 고급 기능 소개
- **통합**: 새 문서의 "성능 최적화" 섹션에 포함

#### **특화 가이드들**
##### `GOLD_LAYER_EXECUTION_GUIDE.md`, `GOLD_LAYER_METRICS_IMPLEMENTATION_GUIDE.md`
- **내용**: Gold Layer 실행 및 메트릭 구현 가이드
- **문제**: JVM 크래시 해결 이전 버전으로 부정확한 정보 포함
- **통합**: 새 문서의 "Gold Layer 구현" 섹션에 올바른 방법으로 재작성

##### `FILE_CLEANUP_PLAN.md`
- **내용**: 파일 정리 계획서
- **상태**: 목적 달성으로 보관
- **학습**: 정리 과정의 체계적 접근법 기록

### 📋 Log Files (22개)

#### **JVM 크래시 로그들**
##### `hs_err_pid*.log` (15개)
- **내용**: JVM SIGSEGV 크래시 덤프
- **원인**: 복잡한 JOIN 연산으로 인한 메모리 폭발
- **분석**: 
  ```
  # A fatal error has been detected by the Java Runtime Environment:
  # SIGSEGV (0xb) at pc=0x00007f8b2c3f4567
  # Problematic frame: V [libjvm.so+0x...]
  ```
- **해결**: JOIN 제거로 문제 완전 해결

##### `replay_pid*.log` (7개)
- **내용**: JVM 크래시 재생 로그
- **용도**: 크래시 상황 재현 및 디버깅
- **결과**: Denormalization 방식으로 근본 해결

##### `derby.log`
- **내용**: Derby 데이터베이스 로그
- **용도**: Hive Metastore 로컬 테스트용
- **상태**: 정상 동작, 프로덕션에서는 PostgreSQL 사용

---

## 4. 기존 Archive 구조

### 📁 `documentation/`
기존에 archive된 문서들의 백업 버전

### 📁 `failed_iceberg_attempts/`
Iceberg 구현 초기 실패 시도들
- `bronze_to_silver_iceberg_local.py` - 로컬 버전
- `bronze_to_silver_iceberg_simple.py` - 단순 버전
- `bronze_to_silver_iceberg_stable.py` - 안정화 시도
- `test_iceberg_basic.py` - 기본 테스트

### 📁 `old_versions/`
프로젝트 초기 버전들
- `bronze_to_silver_pipeline.py` - 초기 파이프라인
- `upload_events_to_s3.py` - 이전 업로드 스크립트

### 📁 `test_files/`
테스트 및 실험 파일들
- `test_spark.py` - Spark 연결 테스트

---

## 5. 파일 복원 방법

### 🔄 개별 파일 복원
```bash
# 특정 Python 파일 복원 (주의: 테스트 목적만)
cp archive/legacy_files/python_experimental/ultra_batch_processor.py .

# 문서 파일 복원
cp archive/legacy_files/markdown_deprecated/ICEBERG_ETL_IMPLEMENTATION_SUMMARY.md .
```

### 📊 로그 분석
```bash
# JVM 크래시 로그 분석
less archive/legacy_files/log_files/hs_err_pid10922.log

# 크래시 패턴 검색
grep -r "SIGSEGV" archive/legacy_files/log_files/
```

### ⚠️ 복원 시 주의사항
1. **테스트 환경에서만 사용**: 프로덕션 환경에서 복원 금지
2. **문제 재현 목적**: 학습이나 디버깅 목적으로만 복원
3. **백업 먼저**: 현재 작동 중인 파일들 백업 후 복원
4. **즉시 되돌리기**: 테스트 완료 후 즉시 archive로 다시 이동

---

## 6. 정리 이유 요약

### 🎯 **핵심 목표 달성**
- **유지보수성 향상**: 22개 → 3개 핵심 파일로 집중
- **신뢰성 확보**: 문제가 있는 코드 제거
- **성능 보장**: 검증된 파일만 유지

### 📊 **정리 통계**
| 카테고리 | 제거된 파일 수 | 주요 이유 |
|----------|---------------|-----------|
| **JVM 크래시** | 1개 | 메모리 폭발로 시스템 불안정 |
| **실험 버전** | 10개 | 안정성 부족, 기능 중복 |
| **빈 파일** | 2개 | 구현되지 않음 |
| **중복 파일** | 3개 | 더 나은 버전으로 대체 |
| **중복 문서** | 7개 | 통합 문서로 대체 |
| **로그 파일** | 22개 | 환경 정리 |

### 🏆 **최종 결과**
- **프로덕션 파일**: `bronze_to_silver_iceberg.py`, `compatible_kst_fact_processor.py`, `upload_to_landing_zone.py`
- **최종 문서**: `COMPLETE_LAKEHOUSE_DOCUMENTATION.md`, `README.md`
- **성공률**: 161,351개 이벤트 처리 (16.1% 완료), JVM 크래시 0건

---

## 📚 학습 포인트

### 💡 **기술적 교훈**
1. **JOIN vs Denormalization**: 복잡한 JOIN은 메모리 폭발 위험
2. **배치 크기 최적화**: 5,000개가 메모리 안전 임계점
3. **KST 시간대 처리**: 한국 사용자 분석의 중요성
4. **Iceberg 스키마 호환성**: 기존 테이블 구조 유지 필요

### 🔧 **개발 프로세스**
1. **점진적 개선**: ultra → smart → improved → compatible
2. **문제 격리**: 각 이슈별 독립적 해결
3. **철저한 테스트**: 35배치 연속 성공으로 안정성 검증
4. **문서화 중요성**: 모든 시행착오와 해결책 기록

### 🚀 **향후 개발 가이드**
1. **메모리 우선**: 성능보다 안정성 우선 고려
2. **단순함 추구**: 복잡한 최적화보다 단순한 해결책
3. **배치 처리**: 대용량 데이터는 반드시 배치로 분할
4. **지속적 모니터링**: JVM 메모리 사용량 실시간 추적

---

**이 Archive는 데이터 엔지니어링 프로젝트의 완전한 개발 히스토리를 담고 있습니다.  
실패와 성공의 모든 과정이 기록되어 있어, 향후 유사한 문제 해결에 귀중한 자료가 될 것입니다.** 📚✨
