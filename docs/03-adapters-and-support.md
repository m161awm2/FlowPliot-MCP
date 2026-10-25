# 03. 어댑터와 지원 구성

상태 값은 두 가지다. 둘 다 `list_capabilities` 결과와 같다.
- `adapter_status`: pmcp가 배포할 수 있는지(implemented / catalog_only)
- `integration.status`: 검증 수준
  - `aws_verified`: 실제 AWS에서 확인함(근거는 `docs/evidence/`)
  - `mock_tested`: moto와 스텁 테스트만 함
  - `not_verified`: 테스트 없음

| pattern_id | 구성 | 어댑터 | 통합 검증 | 비고 |
|---|---|---|---|---|
| `s3_athena_iceberg_batch` | S3 → Athena SQL(MERGE) → S3 Iceberg / Scheduler → Step Functions / CloudWatch → SNS | implemented | mock_tested → 실제 검증 진행 중 | 기존 저장소 안에서 SQL로 가공하는 경로. upsert, 삭제, 창 재처리, 문제 파일 격리 |
| `kinesis_firehose_s3` | KDS → Firehose(+Lambda 변환) → S3(JSON gzip/Parquet) + Glue 테이블 | implemented | mock_tested | append 전용. errors/ 재주입 복구 |
| `kinesis_lambda_dynamodb` | KDS → Lambda → DynamoDB(조건부 쓰기) / 실패 배치는 SQS에 기록 | implemented | mock_tested | 키로 조회하는 소비자용. 재생해도 결과 같음 |
| `s3_glue_spark_batch` | S3 → Glue 5.0 Spark → Iceberg MERGE / Scheduler → Step Functions | implemented | mock_tested(렌더링·lint만) | 작업 스크립트를 실제로 실행해 본 적 없음 |
| `s3_emr_serverless_spark` | S3 → EMR Serverless → Iceberg | catalog_only | not_verified | 필요한 작업: 애플리케이션과 실행 역할 템플릿, `emr-serverless:startJobRun.sync`, 로그 진단 |
| `redshift_sql_elt` | Redshift 안에서 SQL ELT | catalog_only | not_verified | 필요한 작업: Data API 비동기 실행기(awslabs redshift 패턴), Scheduler 대상, DB 사용자 권한으로 읽기 검증 |
| `kinesis_firehose_iceberg_upsert` | Firehose Iceberg 목적지(행 단위 upsert/delete) | catalog_only | not_verified | 필요한 작업: IcebergDestinationConfiguration, 고유 키, 압축 스케줄 |
| `kinesis_flink_stateful` | Managed Service for Apache Flink | catalog_only | not_verified | 필요한 작업: 애플리케이션 패키징, savepoint 기반 복구 |
| `dms_cdc_s3` | RDS → DMS CDC → S3 | catalog_only | not_verified | 필요한 작업: DMS Serverless, 네트워크 도달 확인 |
| `opensearch_ingestion` | OSI → OpenSearch | catalog_only | not_verified | 필요한 작업: OSI 파이프라인, 인덱스 템플릿 |

## 공통 동작 (구현된 어댑터)
- **이름과 태그**:
  - 모든 리소스 이름은 `pmcp-<flow 10자>` 형식이다. Glue는 `pmcp_<10자>`.
  - 태그는 `managed-by=pipeline-mcp`, `pmcp:owner=<sub>`, `pmcp:flow=<id>`. 스택 태그가 하위 리소스로 전파된다.
- **IAM**:
  - 흐름이 만드는 역할에는 모두 `pmcp-boundary` 권한 경계가 붙는다.
  - 소비자 역할 `pmcp-<id>-consumer`는 플랫폼 역할이 ExternalId로만 AssumeRole할 수 있고, 출력만 읽을 수 있다.
- **상시 감시(LLM 없이 동작)**:
  - Athena/Glue: 실패 실행 알람, 일정 시간 안에 성공 실행이 없을 때 알람
  - Firehose: DataFreshness, 변환 실패(EMF 지표), Lambda 오류 알람
  - DynamoDB: Lambda 오류, IteratorAge, 실패 배치 알람
- **비용 보호**: Athena 작업 그룹에 조회당 스캔 한도를 둔다(기본 10 GB).

## 지원하지 않는 조합 (UNSUPPORTED_CONFIGURATION으로 거부, 테스트로 확인)
- Athena:
  - 변환 SQL에 DML, 여러 문장, 알 수 없는 placeholder가 있거나 `{{source}}`가 없는 경우
  - 계약에 없는 원천
  - `upsert_with_deletes`인데 `pmcp_deleted` 출력이 없는 경우
  - 스케줄 간격이 최신성 요구보다 긴 경우
  - 창 길이가 스케줄 간격보다 짧은 경우
  - 키가 없는 append인데 시간 컬럼도 없는 경우
  - 컬럼 삭제, 타입 변경, 키·파티션 변경을 `allow_destructive_rebuild` 없이 요청한 경우
- Firehose: upsert/delete 계약, 허용 목록 밖의 import를 쓰는 변환 코드, Parquet인데 버퍼가 64 MB 미만인 경우
- DynamoDB: 키가 없거나 3개 이상인 경우, 정렬 컬럼이 없는 경우
- 모든 패턴: 패턴 변경(제자리 이전 불가. 새 흐름을 만든 뒤 기존 흐름을 정리)

## 에이전트가 작성하는 코드 검사 (`adapters/codegen.py`)
- Lambda 변환 코드(`transform(record)`)와 PySpark 변환 코드(`transform(spark, df, start, end)`)는 AST 허용 목록으로
  검사한다.
- 허용하는 import: 순수 표준 라이브러리. PySpark 변환은 `pyspark`도 허용한다.
- 금지: `open`, `eval`, `exec`, `__import__`, dunder 속성 접근, 최상위의 실행문.
- 이 코드는 사용자 계정의 Lambda나 Glue에서만 실행된다. 플랫폼에서는 실행하지 않는다.
