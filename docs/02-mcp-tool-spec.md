# 02. MCP 도구 명세 (20개)

정확한 입력·출력 JSON 스키마는 `docs/tool-schemas.json`에 있다. `pmcp tool-spec`으로 다시 만든다.
이 문서는 스키마로 표현되지 않는 의미를 적는다: 언제 쓰는지, 비슷한 도구와 무엇이 다른지, 오류, 부작용, 다음 작업.

## 공통 규칙
- **호출자**: 클라우드 모드에서는 JWT의 `sub`가 사용자다. 모든 기록과 AWS 접근은 이 사용자로 한정된다.
- **오류 형식**: `is_error=true`이고, 텍스트는 `Error executing tool <name>: {"error": {...}}`다.
  - 앞부분은 MCP SDK가 붙인다.
  - JSON에는 `code`, `message`, `retryable`, 그리고 선택적으로 `details`, `next_actions`가 들어 있다.
- **오류 코드**:

  | 코드 | 의미 |
  |---|---|
  | `INVALID_INPUT` | 입력값이 형식에 맞지 않음 |
  | `NOT_FOUND` | 대상이 없음 |
  | `FORBIDDEN` | 권한 없음 |
  | `NOT_CONNECTED` | 연결되지 않은 계정 |
  | `WRITE_ACCESS_NOT_ENABLED` | 읽기 전용으로 연결된 계정 |
  | `UNSUPPORTED_CONFIGURATION` | 배포할 수 없는 구성 조합 |
  | `PRECONDITION_FAILED` | 실행 전제조건이 맞지 않음 |
  | `STALE_STATE` | 검토 이후 대상이 바뀜 |
  | `APPROVAL_REQUIRED` | 사람의 승인이 필요함 |
  | `DIGEST_MISMATCH` | 승인된 내용과 다름 |
  | `PLAN_EXPIRED` | 계획 만료 |
  | `PLAN_NOT_EXECUTABLE` | 실행할 수 없는 계획 상태 |
  | `CONFLICT` | 동시 작업과 충돌 (재시도 가능) |
  | `QUOTA_EXCEEDED` | 동시 작업 한도 초과 |
  | `COST_CAP_EXCEEDED` | 비용 상한 초과 |
  | `RATE_LIMITED` | 요청 횟수 제한 (재시도 가능) |
  | `AWS_ACCESS_DENIED` | AWS가 접근을 거부함 |
  | `AWS_ERROR` | 기타 AWS 오류 (재시도 가능) |
  | `TIMEOUT` | 시간 초과 (재시도 가능) |
  | `INTERNAL` | 내부 오류 |
- **도구 성격 표시(annotations)**: `readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`를 붙였다.
  `apply_changes`와 `execute_recovery`만 `destructive`다.
- **승인 도구는 없다.** 승인은 웹 `/approve/<plan_id>` 또는 로컬 터미널 `pmcp approve`에서만 할 수 있다.
- **장시간 작업**: `operation_id`를 받아 `get_operation`으로 조회한다. MCP tasks나 진행률 알림 같은 클라이언트
  기능에 의존하지 않는다.

## 조사

### list_workspace (추가)
- **사용 상황**: 새 세션을 시작할 때. 연결된 계정(리전, 쓰기 권한, 비용 상한), 흐름, 승인 대기 계획, 최근 작업,
  열린 장애를 보여준다.
- **비슷한 도구와 차이**: `inspect_aws_environment`는 AWS를 호출해 계정의 리소스를 본다. 이 도구는 pmcp 상태만
  읽는다.
- **입력**: 없음.
- **부작용**: 없음.
- **다음 작업**: ID를 골라 다른 도구를 호출한다. 계정이 없으면 `connect_url`로 안내한다.

### inspect_repository
- **사용 상황**: 데이터가 어디서 생기는지 코드로 알아낼 때.
- **입력**: `source`(https git URL 또는 연결 계정의 `s3://` 아카이브). 비공개 저장소는 `token_secret_arn`
  (사용자 계정의 `pmcp/*` 시크릿).
- **출력**:
  - 생산자 호출(Kinesis/Firehose/S3/Kafka/SQS/DynamoDB/SQL)의 file:line
  - 리소스 이름 힌트, 이벤트 필드 후보
  - 스키마 파일, 샘플 파일, IaC 파일
- **제한**: 저장소 50 MB, 파일 1 MB, git clone 25초.
- **오류**: `INVALID_INPUT`(형식, 크기, clone 실패), `TIMEOUT`, `FORBIDDEN`(다른 계정의 시크릿).
- **부작용**: 임시 디렉터리에 받았다가 지운다.
- **다음 작업**: `profile_source`.

### inspect_aws_environment
- **사용 상황**: 재사용할 수 있는 리소스와 제약을 볼 때. Lake Formation 모드를 확인해 `lake_formation_grants`
  값을 정할 때도 쓴다.
- **비슷한 도구와 차이**: 이름과 메타데이터만 본다. 데이터 모양은 `profile_source`가 본다.
- **입력**: `account_id`, `region`, `categories?`
- **출력**: 범주별 목록과, 권한 부족 등으로 범주별 실패한 내역(`errors`).
- **부작용**: 없음. reader 역할로 읽기만 한다.

### profile_source
- **사용 상황**: 원천 데이터의 크기·증가율·형식·스키마·키 후보·최신성을 잴 때.
- **입력**: `source_uri`(`s3://`, `glue:<db>.<table>`, `kinesis:<stream>`, `dynamodb:<table>`),
  `sample_records`(10~2000), `include_raw_samples`(기본 false).
- **출력**:
  - 프로파일: 객체 수, 일별 바이트, 파티션 키, 컬럼 추론, null 비율, 키·이벤트 시각·삭제 표시 후보, 중복,
    마스킹된 샘플
  - `suggested_source_columns`: 계약 형식의 컬럼 목록
- **제한**: Parquet 내용은 파싱하지 않고 `schema: unverified`를 반환한다.
- **부작용**: 없음. 원본 값은 사용자가 요청할 때만 반환한다.

### inspect_consumer_contract
- **사용 상황**: 이미 있는 목적지를 바꾸기 전에 소비자에게 미칠 영향을 볼 때.
- **입력**: `target`(`glue:<db>.<table>` 또는 `dynamodb:<table>`), `principal_arn?`, `contract_id?`
- **출력**:
  - 현재 스키마, 파티션, 형식, 최신 데이터 시각
  - principal의 IAM 시뮬레이션 결과. 버킷 정책, KMS, Lake Formation은 포함되지 않는다고 명시한다.
  - 계약과 비교한 결과
- **비슷한 도구와 차이**: 실제로 읽어 보는 확인은 `verify_data_flow`가 흐름의 소비자 역할로 한다.

## 계약과 설계 지원

### define_data_contract (추가)
- **사용 상황**: 사용자와 합의한 요구사항을 저장할 때.
- **입력**: `contract`(원천, 출력 스키마, 키와 변경 방식, 최신성, 변환 복잡도, 볼륨, 소비자, 품질 규칙,
  재처리 창, 비용 상한), `contract_id?`, `expected_version?`
- **검증**: 키·정렬 컬럼·파티션이 출력 컬럼에 있는지, upsert 모드에 키와 정렬 컬럼이 있는지 등.
- **출력**: `contract_id`, `contract_version`, `created_new_version`, `derived_requirements`.
  본문이 같으면 같은 버전을 돌려준다.
- **오류**: `INVALID_INPUT`, `CONFLICT`(다른 버전을 기준으로 수정한 경우).
- **부작용**: pmcp 상태만 쓴다.

### list_capabilities
- **사용 상황**: 어떤 패턴이 있고, 각각 배포할 수 있는지와 검증 수준을 볼 때. `pattern_id`를 주면
  `params_schema`도 준다.
- **비슷한 도구와 차이**: 계약에 대한 적합성은 `validate_architecture`가 판단한다.

### get_service_constraints
- **사용 상황**: 요구가 특정 서비스의 한계(처리량, 레코드 크기, 실행 시간)에 가까울 때.
- **출력**: 문서화된 기본값(`kind=documented_default`), 동작 특성, 리전 가용성(botocore 데이터).
  `account_id`를 주면 Service Quotas 실제 값도 준다.

### estimate_cost
- **사용 상황**: 구현된 패턴 하나에 구체적인 파라미터를 넣어 월 비용을 비교할 때.
- **출력**: 항목별 단가, 단가 출처(`pricing_api` 또는 `static_table`)와 기준일, 가정, 신뢰도.
- **오류**: catalog_only 패턴은 `UNSUPPORTED_CONFIGURATION`. 비용 모델이 없다는 사실과 구현 작업을 알려준다.

### validate_architecture
- **사용 상황**: 후보 비교. 이 결과를 근거로 사용자에게 선택지를 설명한다.
- **입력**: `contract_id`, `candidates[{pattern_id, pattern_params?}]`(최대 8개), `account_id?`, `region?`
- **출력(후보별)**: `fitness`(수준, 근거, 차단 사유, 미확정 사항), `adapter_status`, `integration`, `params_valid`,
  `cost`, `deployable_now`, `deploy_blockers`.
- **출력(전체)**: `best_fit`, `best_deployable_now`, `better_fit_not_proposed`
- **규칙**: 구현됐다는 이유만으로 최적이라고 하지 않는다. 둘이 다르면 `next_actions`에서 차이를 설명하라고 안내한다.

## 변경

### prepare_changes
- **사용 상황**: 새 흐름을 만들거나, `flow_id`를 주어 기존 흐름을 수정할 때.
- **입력**: `account_id`, `region`, `contract_id`, `pattern_id`, `pattern_params`, `flow_id?`, `change_reason?`,
  `reprocess{mode: auto|none|window}`
- **출력(PlanView)**:
  - 대상(계정, 리전, 스택), 현재 상태와 원하는 상태
  - 리소스 diff, 코드·SQL diff
  - `data_impact`: 스키마 변경, 소비자 영향, 재처리 필요 여부와 창, 데이터·인프라 롤백 방법
  - 비용, 실행 순서(선행 조건, 실패 시 처리, 되돌릴 수 있는지), 경고, digest
- **오류**:
  - `UNSUPPORTED_CONFIGURATION`: 파라미터 조합 불가, cfn-lint 실패, 제자리에서 할 수 없는 스키마 변경,
    패턴 변경
  - `PRECONDITION_FAILED`: 스택이 다른 작업 중
  - `NOT_CONNECTED`, `FORBIDDEN`
- **부작용**: 계획 레코드만 만든다. AWS는 읽기만 한다.

### preview_changes
- **사용 상황**: 계획을 승인할 수 있는 정확한 형태로 만들 때.
- **부작용**:
  - 결과물을 사용자 계정의 `pmcp-artifacts-*` 버킷에 올린다(내용 해시가 키).
  - CloudFormation ChangeSet을 만든다. 리소스는 아직 바뀌지 않는다.
  - 새 스택이면 `REVIEW_IN_PROGRESS` 상태가 된다.
- **출력**: 실제 변경과 교체 여부, `plan_digest`, `approval`(승인 링크).
- **오류**: `STALE_STATE`(계획은 superseded), `PLAN_EXPIRED`, `TIMEOUT`(ChangeSet 생성 중; 다시 호출).
- **읽기 전용 계정**: ChangeSet을 만들지 않고 경고와 함께 previewed 상태가 된다. apply는 할 수 없다.

### apply_changes
- **입력**: `plan_id`, `plan_digest`, `idempotency_key`(8자 이상)
- **실행 전 확인**:
  - 승인 기록이 같은 digest인지
  - 저장된 내용을 다시 해시한 값이 같은지
  - 만료되지 않았는지
  - 대상 fingerprint가 같은지
  - ChangeSet이 AVAILABLE인지
  - 흐름 잠금, 동시 작업 한도, 비용 상한
- **출력**: `OperationView`(`operation_id`)
- **멱등성**:
  - 같은 키로 다시 호출하면 같은 operation을 돌려준다. 워커가 죽었으면 다시 띄운다.
  - 같은 키를 다른 계획에 쓰면 `CONFLICT`.
  - 다른 키로 같은 계획을 실행하면 기존 operation을 돌려준다.
- **부작용**: 사용자 계정에 ChangeSet 실행, 데이터 단계(DDL, 재처리) 수행.
- **다음 작업**: `get_operation` → `verify_data_flow`. 성공은 배포 완료일 뿐 검증 완료가 아니다.

## 검증

### verify_data_flow
- **입력**: `flow_id`, `window?{start,end}`, `idempotency_key?`
- **기본 창**: 배치는 마지막 성공 실행의 창, 스트리밍은 최근 1시간.
- **확인 항목**: 존재, 스키마, 키, 기대 건수와 실제 건수, 최신 버전·삭제 반영, 최신성, 원본 대비 진행,
  품질 규칙, 소비자 역할로 실제 읽기.
- **출력**: operation. 결과는 `get_operation`의 `verification.checks[]`에 담긴다. 각 항목에 상태, 창, 기대값, 관측값,
  증거(Athena query id, CloudWatch 지표), 사유가 들어 있다.
- **부작용**: 사용자 계정에서 Athena 조회를 실행한다(소량 비용). 흐름 상태를 `verified`, `deployed`, `degraded`로
  갱신한다.

## 운영

### observe_flow
- 스택, 최근 실행·처리량, 알람, 스케줄 상태, 마지막 성공 이후 경과 시간, 마지막 검증, 진행 중인 작업을 보여준다.
- `include_cost=true`이면 Cost Explorer를 호출한다. 사용자 계정에 호출당 0.01달러가 붙는다.
- 읽기 전용이다. 문제가 있으면 `diagnose_failure`.

### diagnose_failure
- 장애(incident)를 연다.
  - `observed_facts`: 사실과 ID
  - `hypotheses`: 가능성 정도, 근거가 되는 사실 ID, 제안 복구
  - `further_checks`: 추가로 확인할 것
  - `affected`: 영향받은 창과 파일
- `deep_probe=true`이면 원인 파일을 찾는 조회를 백그라운드 operation으로 실행한다(Athena, 소량 비용).
- **부작용**: 장애 레코드를 만든다.

### plan_recovery
- **입력**: `incident_id`, `strategy?`(기본값은 최상위 가설의 제안)
- **출력**: 복구 계획과 `available_strategies`.
  - 인프라 복원(`restores_infrastructure`)과 데이터 복구를 나눠 보여준다.
  - 모든 복구는 마지막에 `verify` 단계가 있다.
- **오류**: `PRECONDITION_FAILED`(격리 전략인데 문제 파일을 아직 못 찾음), `INVALID_INPUT`(쓸 수 없는 전략).

### execute_recovery
- `apply_changes`와 같은 확인을 거친다. 복구 계획만 받는다(종류가 다르면 `INVALID_INPUT`).
- 실행 후 다시 검증하고, 검증이 실패하면 operation이 실패한다.

### get_operation (추가)
- 단계별 상태와 오류, 결과를 보여준다. 검증이면 전체 `checks`도 포함한다.
- `stalled=true`는 워커가 없다는 뜻이다. 같은 `idempotency_key`로 시작 도구를 다시 호출하면 이어서 실행한다.

## 기본 호출 순서
| 목표 | 호출 순서 |
|---|---|
| 신규 생성 | list_workspace → profile_source → define_data_contract → validate_architecture → prepare_changes → preview_changes → (사람 승인) → apply_changes → get_operation → verify_data_flow → get_operation |
| 규칙 수정 | prepare_changes(flow_id) → preview_changes → (승인) → apply_changes(재처리 포함) → get_operation → verify_data_flow |
| 장애 | observe_flow → diagnose_failure(deep_probe) → get_operation → plan_recovery → preview_changes → (승인) → execute_recovery → get_operation |

## 실패 실행 상세 조회

`get_run_failure(flow_id, execution, next_token?)`는 실행 이름 또는 ARN으로 기존 Step Functions 이력과 Athena 오류를 읽는다. 새 incident, 쿼리, 복구 작업을 생성하지 않는다. `athena_queries`에는 query_execution_id, state_change_reason, athena_error가 담긴다. `cause`와 실패 이벤트의 원문은 자르지 않는다. 이력은 100개 이벤트 단위이며 next_token이 있으면 같은 실행으로 이어서 조회한다. 실행의 flow 소속, 호출자 계정 접근 권한 및 Athena workgroup을 검사한다.

`diagnose_failure`는 동일 증거의 미해결 incident를 재사용한다. 기존 중복 기록은 자동 삭제하지 않는다.
