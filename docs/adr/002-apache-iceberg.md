# ADR 002. Apache Iceberg Table Format 선택

- **날짜**: 2025.09
- **상태**: 승인됨

---

## 배경

Kafka → S3 증분 적재 환경에서 데이터를 저장하고 쿼리할 포맷을 선택해야 했습니다.
주요 후보는 세 가지였습니다.

- **Option A**: Apache Iceberg
- **Option B**: Delta Lake
- **Option C**: Apache Hudi

---

## 의사결정

**Apache Iceberg(Option A)를 선택합니다.**

### 근거

1. **Trino 연동 성숙도**: 세 포맷 중 Trino와의 연동이 가장 안정적이고 성숙함
2. **ACID 트랜잭션**: Kafka at-least-once 전송으로 발생하는 중복 데이터 문제를 트랜잭션 단위로 처리 가능
3. **스키마 진화(Schema Evolution)**: AB 테스트 등 서비스 업데이트로 인한 스키마 변경에 유연하게 대응 가능
4. **Partition Pruning**: 날짜 기반 파티셔닝으로 Daily 쿼리 I/O 비용 최소화
5. **저장과 연산 분리**: Hive Metastore를 카탈로그로 연동하여 스토리지(S3)와 쿼리 엔진(Trino)을 독립적으로 확장 가능

### Delta Lake, Hudi 대비 비교

| 항목 | Iceberg | Delta Lake | Hudi |
|------|---------|-----------|------|
| Trino 연동 | 우수 | 보통 | 보통 |
| ACID 트랜잭션 | 지원 | 지원 | 지원 |
| 스키마 진화 | 유연 | 유연 | 제한적 |
| Partition Pruning | 강력 | 강력 | 보통 |
| 커뮤니티 성숙도 | 높음 | 높음 | 중간 |

---

## 결과

- Bronze→Silver 전환 시 ACID 트랜잭션으로 **데이터 정합성 99.31%** 달성
- Partition Pruning으로 Daily 쿼리 I/O 비용 **96.84% 절감** (전체 데이터셋의 3.16%만 스캔)
- Iceberg Compaction(`rewrite_data_files`)으로 Small File 문제 해소

## 트레이드오프

| 항목 | 내용 |
|------|------|
| 장점 | Trino 연동 최적, ACID 보장, Partition Pruning 강력 |
| 단점 | 초기 설정 복잡도 높음 (Hive Metastore 별도 구성 필요) |
| 미해결 과제 | APPEND 방식의 재실행 멱등성 미달성 → Dynamic Partition Overwrite 도입 예정 |
