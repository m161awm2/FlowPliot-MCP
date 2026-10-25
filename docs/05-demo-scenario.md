# 05. 발표 데모 시나리오

**원칙**
- 화면은 실제 AWS(계정 608420805531, ap-southeast-2)에서 나온 결과를 쓴다.
- 모의 결과를 보여줄 때는 화면에 **[모의]**라고 표시한다.
- 승인 단계는 사람이 브라우저에서 직접 한다. 에이전트에게는 승인 도구가 없다는 점을 데모에서 보여준다.

## 0. 준비 (1분)
- `https://mcp.m161awm.kr` 가입, 토큰 발급, 계정 연결 화면을 보여준다.
- 에이전트 등록 명령을 보여준다(Claude Code 또는 Codex).
- 파이프라인 구조도:
  ```
  사용자 계정
    S3 원천 ──▶ Glue 원천 테이블 ──▶ Athena MERGE ──▶ Iceberg 출력 ──▶ 소비자 역할로 읽기
    EventBridge Scheduler ─▶ Step Functions ─▶ (위 MERGE 실행)
    CloudWatch 알람 ─▶ SNS
  플랫폼 계정
    CloudFront ─▶ ALB ─▶ Fargate(pmcp) ─▶ AssumeRole + ExternalId ─▶ 사용자 계정
  ```

## 1. 요구사항 → 후보 비교 → 계획 (시나리오 1, 5)
1. 프롬프트: "s3://pmcp-e2e-src-608420805531/orders/의 주문 이벤트로 BI용 주문 테이블을 만들어 줘. 수정과 삭제를
   반영하고, 1시간 이내 최신성이면 돼."
2. 에이전트 호출 순서: `profile_source` → `define_data_contract` → `validate_architecture`
   - 화면: 패턴별 적합성과 근거, `adapter_status`, `integration`, `best_fit`과 `best_deployable_now`
3. 같은 계약에서 최신성만 **30초**로 바꿔 다시 `validate_architecture`를 호출한다.
   - Athena 패턴이 unsuitable로 바뀌고, 근거로 "freshness below ~5 min floor"가 나온다.
   - 스트리밍 패턴이 최적으로 올라간다(시나리오 5).
4. 상태가 있는 스트리밍 요구로 바꾸면 최적은 Flink(catalog_only)다. 화면에 `deployable_now=false`와 필요한 구현
   작업이 나온다(시나리오 6).

## 2. 생성 → 실행 → 적재 검증 (시나리오 2)
1. `prepare_changes` → `preview_changes`
   - 화면: CloudFormation 리소스 diff, MERGE SQL, 비용, 실행 순서, digest
2. 에이전트가 승인 없이 `apply_changes`를 부르면 `APPROVAL_REQUIRED`가 나온다. 이 장면을 보여준다.
3. 브라우저 `/approve/<plan_id>`에서 승인한다. 로그인한 지 오래됐으면 다시 로그인을 요구한다.
4. `apply_changes` → `get_operation`
   - 단계 진행: precheck → ChangeSet 실행 → 스택 완료 → 테이블 생성 → 흐름 기록
5. 첫 창을 실행하고 `verify_data_flow` → `get_operation`
   - 확인 항목 목록: 건수, 중복 키 0, 최신 버전과 삭제 반영, 최신성, 소비자 역할 읽기
   - 원천 ground truth(`ground-truth/orders.json`, 살아 있는 주문 수)와 대조한다.
6. Athena 콘솔에서 `pmcp-<id>-consumer` 작업 그룹으로 직접 SELECT해 실제 적재 결과를 보여준다.

## 3. 가공 규칙 수정 → 영향 → 재처리 → 검증 (시나리오 3)
1. 프롬프트: "주문에 채널(channel) 컬럼을 추가하고, 금액은 소수 둘째 자리로 반올림해."
2. 새 계약 버전으로 `prepare_changes(flow_id)`를 호출한다. 화면:
   - `add column channel`
   - SQL diff
   - 재처리 필요(사유와 창)
   - 소비자 영향, `data_rollback=rebuild_from_source`
3. 승인 → 실행(ALTER ADD COLUMNS → 창 재처리) → 검증

## 4. 장애 → 증거 → 진단 → 복구 → 재검증 (시나리오 4)
1. 형식이 깨진 파일을 넣는다: `scripts/gen_orders.py --bad-file`. 다음 실행이 실패하고 알람이 울린다.
2. `observe_flow`: ALARM 상태와 실패 실행
3. `diagnose_failure(deep_probe=true)`
   - 관측 사실: 실패 실행과 HIVE_BAD_DATA 오류 발췌
   - 가설: 파싱할 수 없는 원천 레코드. 근거 사실 ID와 함께 표시한다.
   - probe 결과: 문제 파일 경로
4. `plan_recovery(quarantine_and_rerun)`
   - 원본 파일은 건드리지 않고 격리 테이블에만 기록한다.
   - 영향받은 창을 재실행하고 검증한다.
   - 인프라 복원은 없다.
5. 승인 → `execute_recovery` → `get_operation`: 검증 결과 verified, 장애 resolved

## 5. 잘못된 요청 처리 (시나리오 7) [모의 + 실제]
- digest가 다른 apply → `DIGEST_MISMATCH`
- 대상이 바뀐 뒤 apply → `STALE_STATE`, 계획 superseded
- 같은 idempotency_key로 두 번 호출 → 같은 operation
- 근거: `tests/test_lifecycle.py` 실행 화면 **[모의]**와 실제 호출 결과(`docs/evidence/`)
