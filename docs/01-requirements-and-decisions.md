# 01. 요구사항과 설계 결정 기록

작성 기준일 2026-10-25. 요구사항 인터뷰(grill-me, Q1~Q44)에서 합의한 내용과 구현 중 바뀐 결정을 기록한다.
"결정 / 근거 / 대안 / 결과" 순서로 쓴다.

## 1. 제품 정의 (합의)

- pmcp는 **여러 사용자가 접속하는 원격 MCP 서버**다. 사용자는 **자기 AWS 계정**을 연결하고, AI 에이전트가 pmcp
  도구로 그 계정 안에 데이터 파이프라인을 설계·생성·수정·검증·운영한다.
- 플랫폼 계정 `608420805531`(ap-southeast-2)은 서버만 운영한다. 파이프라인 리소스, 데이터 처리, 상시 감시, 비용은
  모두 **사용자 계정 안**에서 생긴다.
- 파이프라인 생성 경로는 최대한 짧게 한다. 데이터 검증은 배포와 분리해 상태로 구분한다(`deployed` → `verified`).
- 명세 9절 시나리오는 제품 테스트·데모용이며 서비스 선택을 제한하지 않는다.

## 2. 결정

### D1. 언어·SDK: Python 3.12 + 공식 `mcp` SDK v2(2.3.0) + boto3 + Pydantic, 패키지는 uv
- 근거: 기존 프로젝트 없음. awslabs/mcp 서버 대부분이 Python, `mcp>=2,<3`이라 패턴과 코드를 그대로 가져올 수 있다.
- 결과: 도구 입력·출력이 Pydantic 모델이고, SDK가 `input_schema`/`output_schema`를 자동 생성한다.

### D2. awslabs/mcp 재사용: 핵심은 직접 구현(c), 유틸은 복사·수정(b), 내부 MCP 클라이언트 호출(a)은 안 함
- 기준 커밋: `awslabs/mcp@24c39705d1d762cb6ebdd72b23f757a67b7a36c2` (2026-10-23).
- 조사 결과(요약):
  - dataprocessing 0.2.2: Glue, Athena, EMR. `operation` 문자열 하나로 여러 작업을 묶고 boto dict를 그대로 받는다.
    명세가 금지한 구조다.
  - redshift 0.1.2: `asyncio.to_thread`와 long-poll 패턴. 가장 쓸 만하다.
  - Kinesis/Firehose/MSK/EventBridge/DMS/Lake Formation/OpenSearch: 전용 서버가 없다. 지원 중단된 `aws-api` 서버로만
    호출할 수 있다.
  - 권한은 프로세스 단위 플래그(`--allow-write`)다. 사용자별 권한이 필요한 멀티테넌트 서버와 맞지 않는다.
- (a)를 버린 이유:
  - 서버별로 프로세스가 따로 떠야 한다.
  - SDK 버전이 섞여 있다(v1, v2, fastmcp).
  - 결과가 텍스트 JSON이라 다시 파싱해야 한다.
  - 테넌트별로 AssumeRole한 세션을 넘길 방법이 없다.
  - 컨테이너 안에서 감당할 비용 대비 이점이 없다.
- (b)로 가져온 것: `pmcp/vendor/awslabs/sql_analyzer.py`, `sensitive_data_filter.py`. 원본 그대로 두고 헤더 주석만
  추가했다. LICENSE, NOTICE, CHANGES.md 포함.
- 패턴만 참고해 다시 구현한 것:
  - `ManagedBy` 태그로 소유 확인 → `pmcp/aws/ownership.py` (pmcp 태그 키와 사용자별 소유자로 확장)
  - `to_thread` 실행기 → `pmcp/aws/client.py::run_sync`
- 비용 추정은 Price List API를 boto3로 직접 호출하고, 실패하면 날짜가 적힌 정적 단가표를 쓴다.
  템플릿 검증은 `cfn-lint` 라이브러리로 한다.

### D3. 서비스 기본값 없음: 적합성과 구현 상태를 분리
- `catalog/patterns.py`: 계약에서 뽑은 요구사항으로 적합성만 계산한다(strong/acceptable/weak/unsuitable + 근거,
  차단 사유, 미확정 사항).
- `adapter_status`(implemented / catalog_only)와 `integration.status`(aws_verified / mock_tested / not_verified)는
  적합성 계산에 섞지 않는다.
- `validate_architecture`는 `best_fit`과 `best_deployable_now`를 따로 반환하고, 후보로 제안되지 않았지만 더 적합한
  패턴도 알려준다(`better_fit_not_proposed`).
- 검증: `tests/test_design.py`
  - 요구 지연을 1시간에서 30초로 바꾸면 Athena가 unsuitable이 되고 Lambda→DynamoDB가 최적이 된다.
  - 상태가 있는 스트리밍 요구에서는 최적이 Flink(catalog_only)인데 배포 가능한 것은 아니라고 보고한다.

### D4. 배포 형태: CloudFront → ALB(HTTPS) → ECS Fargate(ARM64), 상태 없는 Streamable HTTP
- 도메인 `mcp.m161awm.kr`은 가비아 DNS라서 루트 도메인에 A 레코드를 CloudFront로 걸 수 없다. 그래서 서브도메인
  CNAME 방식이다. 오리진 `origin-mcp.m161awm.kr`도 HTTPS다.
- ALB는 CloudFront IP 대역만 받는다. 리스너 규칙은 비밀 헤더(`X-Pmcp-Origin-Verify`)가 없으면 403을 돌려준다.
  앱(`OriginVerify`)에서도 같은 확인을 한다.
- NAT Gateway는 쓰지 않는다. 퍼블릭 서브넷에서 퍼블릭 IP를 받는다.
- 서버는 0.25 vCPU / 0.5 GB이고 CPU에 따라 1~3개로 자동 조정한다. WAF는 쓰지 않는다.
- 요청 제한은 앱에서 사용자당 분당 60회다.
- `json_response=True`, `stateless_http=True`: 태스크가 여러 개여도 세션 고정이 필요 없고, CloudFront 30~60초
  타임아웃 안에서 응답이 끝난다.

### D5. 장시간 작업: 일회성 ECS RunTask 워커 (SQS, 상시 워커 없음)
- 사용자 요청: "SQS는 되도록 사용하지 말고, 서버는 단순하게 비용을 아껴 달라."
- 동작 방식:
  - `apply_changes` 같은 도구는 operation을 기록하고 RunTask로 워커를 띄운 뒤 `operation_id`를 바로 돌려준다.
  - 워커는 단계가 끝날 때마다 DynamoDB에 체크포인트를 남긴다.
  - lease(90초, 하트비트 갱신)로 워커가 하나만 돌게 한다.
  - 같은 `idempotency_key`로 다시 호출하면 죽은 워커를 다시 띄워 마지막 완료 단계 다음부터 이어간다.
- 각 단계 자체가 멱등이다: ChangeSet 실행에 `ClientRequestToken`, Step Functions 실행 이름을 고정,
  DDL은 `IF NOT EXISTS`.
- 로컬 모드는 같은 코드를 스레드로 실행한다.
- 파이프라인의 정상 실행과 감시는 사용자 계정 안에서 돈다(Scheduler/Step Functions/Firehose/Lambda +
  CloudWatch 알람 → SNS). pmcp나 LLM이 없어도 계속 동작한다.

### D6. 상태 저장: 플랫폼 DynamoDB 단일 테이블 + 조건부 쓰기
- 키는 `pk=U#<사용자 sub>`, `sk=<KIND>#<id>`다. 모든 기록은 사용자 단위로 나뉘어 다른 사용자 것을 읽을 수 없다.
- 버전 비교 쓰기(CAS)로 동시 수정을 막는다. 멱등 키, 흐름 잠금, lease는 TTL이 있는 claim 항목이다.
- 플랫폼에는 메타데이터, 해시, 통계, 증거의 S3 URI만 둔다.
- **변경 사항(Q40 보완)**: 생성된 템플릿·SQL·코드는 계획 레코드 안에도 복사본을 둔다. 승인된 내용과 똑같은 것을
  다시 실행하고 diff를 보여주기 위해서다. 고객 **데이터**는 아니다. 배포되는 사본은 사용자 계정의
  `pmcp-artifacts-<acct>-<region>` 버킷에 내용 해시를 키로 올라간다.

### D7. 인증: Cognito 직접 가입 → 서버가 RS256 JWT 발급, 승인은 브라우저에서 새로 로그인해야 함
- MCP 토큰: `aud=pmcp-mcp`, `scope=mcp`, 7일. `jti`로 폐기할 수 있다.
- 웹 세션: `aud=pmcp-web`, 1시간, CSRF 값 포함. Cognito 콜백에서만 발급된다.
- 승인 페이지 `/approve/<plan_id>`는 로그인한 지 10분 이내여야 열린다. 오래됐으면 Cognito 로그아웃 후 다시
  로그인시킨다.
- MCP 토큰은 쿠키로 넣든 Bearer로 넣든 승인 페이지에서 거부된다(테스트 확인).
- 모델이 `approved=true`를 넣어 승인하는 구조가 아니다. 승인 도구 자체가 없다.
- Cognito를 MCP OAuth 서버로 쓰지 않은 이유: 클라이언트 동적 등록(DCR)을 지원하지 않는다.

### D8. 사용자 계정 연결: CloudFormation 빠른 생성 + ExternalId, 액세스 키는 받지 않음
- 연결 스택이 만드는 것:
  - `pmcp-reader`: 필수
  - `pmcp-deployer`, `pmcp-cfn-exec`: 선택
  - `pmcp-boundary`: 권한 경계 관리형 정책
  - `pmcp-artifacts-*`: 결과물 버킷
- 신뢰 정책: 플랫폼 태스크 역할만, 그리고 사용자별 ExternalId가 맞을 때만 AssumeRole할 수 있다.
- deployer의 권한 제한:
  - `pmcp-*` 스택만 다룬다.
  - ChangeSet은 `pmcp-cfn-exec`을 지정해야만 만들 수 있다(Deny 조건).
  - 데이터 단계는 `pmcp-*` 리소스에만 할 수 있다.
- `pmcp-cfn-exec`은 권한 경계가 붙은 `pmcp-*` 역할만 만들 수 있다.

### D9. 승인과 실행이 같은 내용임을 보장: digest + 대상 fingerprint + ChangeSet
- digest 대상: 계획 ID, 사용자, 대상, 패턴 파라미터, 계약 버전, 결과물 해시, 단계, 대상 fingerprint, ChangeSet ID,
  만료 시각.
- `apply_changes`가 거부하는 경우:
  - digest가 다르다.
  - 저장된 내용을 다시 해시한 값이 다르다(변조).
  - 승인 기록이 없다.
  - 만료됐다.
  - 대상 스택 상태나 흐름 설정이 바뀌었다(STALE_STATE). 이 경우 계획을 `superseded`로 바꾼다.
  - ChangeSet이 AVAILABLE 상태가 아니다.
  - 비용 상한을 넘는다.
  - 동시 작업 한도를 넘는다.
- preview에서 CREATE ChangeSet을 만들면 스택이 `REVIEW_IN_PROGRESS`로 바뀐다(moto 테스트에서 발견). 그래서
  fingerprint는 preview **이후** 상태로 다시 잡고 digest에 넣는다.

### D10. 인프라 복구와 데이터 복구는 따로 다룸
- 인프라: CloudFormation이 자동 롤백한다. 복구 계획의 `restore_template`은 마지막으로 승인된 템플릿을 다시 적용한다.
- **한계**: 템플릿이 같으면 CloudFormation은 해당 리소스를 바꾸지 않는다. 그래서 콘솔에서 직접 고친 설정(drift)은
  되돌리지 못한다. 계획의 notes에 이 사실을 적는다.
- 데이터:
  - Athena 패턴은 같은 창을 다시 돌려도 결과가 같다(MERGE, 또는 창 삭제 후 삽입).
  - 원본 파일은 수정하지 않는다. 문제 파일은 흐름 전용 `pmcp_quarantine` 테이블에 기록해 읽을 때 제외한다.
  - Firehose 패턴은 errors/ 객체를 스트림에 다시 넣고 원본 객체는 보관 위치로 옮긴다.
  - DynamoDB 패턴은 실패 배치를 스트림에서 다시 읽어 넣는다(조건부 쓰기라 중복 무해).
  - 스냅샷 롤백은 시도하지 않는다. 계획에 `data_rollback=rebuild_from_source | not_reversible`로 명시한다.

### D11. 검증은 데이터 자체를 본다
- `verify_data_flow`가 확인하는 것:
  - 존재, 스키마·타입, 키 유일성
  - 창 기준 기대 건수 대비 실제 건수. 기대값은 계약의 변환을 원본에 적용해 계산한다.
  - 최신 버전과 삭제가 반영됐는지
  - 최신성과 원본 대비 진행 정도
  - 계약의 품질 규칙
  - **소비자 역할(`pmcp-*-consumer`)로 AssumeRole해서 실제 읽기**
- 확인할 수 없는 항목은 `unverified`로 사유와 함께 반환한다. 판정은 `verified / partially_verified / failed`다.
- 창이 없거나 아직 실행 이력이 없으면 `partially_verified`이고, 흐름 상태는 `verified`로 올리지 않는다.

### D12. 도구 수: 16 → 19 (근거는 `02-mcp-tool-spec.md`)
- `define_data_contract`: 계약을 ID와 버전으로 저장한다. 없으면 모든 도구가 거대한 자유 형식 JSON을 받게 된다.
- `get_operation`: 장시간 작업의 상태를 조회한다. `observe_flow`는 흐름 상태를 보는 도구라 역할이 다르다.
- `list_workspace`: 새 세션에서 계정, 흐름, 대기 중 계획의 ID를 찾는다. 이게 없으면 에이전트가 ID를 얻을
  방법이 없다.
- 도구 정의 총량: 처음 69.9 KB였다. 계획·작업 출력 스키마를 평탄화해 55.1 KB로 줄였다(`docs/tool-schemas.json`
  기준). 지연 로딩이 없는 클라이언트를 고려한 조치다. 남은 양의 대부분(약 8.7 KB)은 `define_data_contract`의
  계약 입력 스키마다.

## 3. 범위 변경 기록
| 변경 | 원인 | 대안 | 영향 / 유지되는 기능 |
|---|---|---|---|
| Glue 패턴의 row_count/mutations는 `unverified` | PySpark 변환에는 SQL로 쓴 기대값이 없음 | 계약에 SQL로 쓴 기대값 규칙을 추가하는 기능(후속) | 존재, 스키마, 키, 소비자 읽기 검증은 유지 |
| Parquet 원본 프로파일링은 `unverified` | 컨테이너 크기 때문에 pyarrow를 넣지 않음 | Glue 테이블을 등록한 뒤 `glue:` URI로 프로파일 | JSON, JSONL, CSV, Kinesis, DynamoDB 프로파일은 유지 |
| `restore_template`은 drift를 되돌리지 못함 | CloudFormation 동작 방식 | drift 감지 후 리소스별로 고치는 기능(후속) | 템플릿 차이 복원과 재실행은 유지 |
| 도구 오류 문자열 앞에 `Error executing tool <name>: ` 붙음 | MCP SDK 2.3.0이 고정으로 붙임 | 없음 | 뒤의 본문은 JSON(`code`, `retryable`, `next_actions`) |

## 4. 남은 위험
- CloudFront `AllViewer` 오리진 요청 정책이 `Authorization` 헤더를 오리진까지 넘기는지는 실제 배포에서 확인해야 한다.
- 플랫폼 상시 비용은 하루 약 1.3~1.5달러다. 예산 30달러로 약 3주를 운영할 수 있다. Budgets 알림은 계정 전체 비용
  기준이라 다른 서비스 비용(예: 기존 EKS 스택)도 함께 집계된다.
