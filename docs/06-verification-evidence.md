# 06. 검증 증거

**모의 테스트**와 **실제 AWS 검증**을 나눠 적는다. 모의 결과를 실제 결과처럼 쓰지 않는다.

## A. 모의 테스트 (moto와 스텁, 실제 AWS 호출 없음)
- 실행: `uv run pytest`
- 결과: **68 passed**. JUnit 결과 파일은 `docs/evidence/mock-tests.xml`(2026-10-25).
- 환경: Python 3.12, mcp 2.3.0, boto3 1.43.118, moto 5.2.4, cfn-lint 1.58.0

| 파일 | 통과 | 확인 내용 | 명세 9절 시나리오 |
|---|---|---|---|
| `test_design.py` | 9 | 요구사항이 바뀌면 선택과 근거가 바뀜(최신성 1시간→30초, 원천 S3→스트림). 최적안이 catalog_only면 `deployable_now=false`. 파라미터가 틀리면 배포 불가. 제안되지 않은 더 적합한 패턴을 알려줌. 계약 버전 관리 | 1, 5, 6 |
| `test_adapters_render.py` | 20 | 구현된 어댑터 4종 템플릿이 cfn-lint 통과. 역할에 권한 경계와 ExternalId가 붙음. `pmcp-` 이름 사용. States.Format 에뮬레이터로 MERGE SQL을 복원해 비교. 변환 SQL과 코드의 허용 목록. 지원하지 않는 조합 거부. 파괴적 스키마 변경은 명시적 허용이 필요. 컬럼 추가는 제자리 변경과 재처리로 처리 | 2, 3, 6 |
| `test_lifecycle.py` | 14 | 모든 단계 생성(moto CloudFormation ChangeSet 실행). 승인 없으면 거부. digest 불일치·변조·종류 불일치. 다른 계획이 먼저 반영되면 STALE_STATE와 superseded. 만료. 중복 apply는 같은 작업이고 데이터 단계는 1회만 실행. 두 번째 워커는 바로 종료. 죽은 워커는 체크포인트부터 이어서 실행. 실패하면 흐름 잠금 해제. 수정 계획의 diff와 재처리. 비용 상한. 읽기 전용 계정. 다른 사용자 격리 | 2, 3, 7 |
| `test_verify_diagnose.py` | 12 | 검증 판정(통과, 행 누락, 삭제 미반영, 소비자 읽기 실패). 실행 이력이 없으면 `partially_verified`. 실패하면 흐름이 `degraded`. 오류 유형 4가지별 가설과 근거 사실 ID. 스케줄 비활성. 장애 → 복구 계획 → 승인 → 실행(격리, 재실행, 검증) → 장애 resolved | 4 |
| `test_stream_adapters.py` | 4 | Firehose 들어온 건수와 처리 건수 대조, 거부 레코드, 트래픽이 없으면 unverified. DynamoDB 키·건수·최신성·소비자 GetItem | 2 |
| `test_mcp_and_auth.py` | 9 | 도구 19개와 annotations, 승인 도구 없음, 오류 JSON 코드, MCP로 승인 없이 apply하면 `APPROVAL_REQUIRED`, 요청 횟수 제한, JWT audience 분리·위조·폐기, 승인 페이지가 MCP 토큰(헤더·쿠키 모두) 거부, 다시 로그인과 CSRF 요구, 다른 사용자는 승인 불가 | 7 |

**모의 테스트의 한계**
- moto는 Athena SQL, Step Functions 실행, Firehose, Lambda를 실제로 수행하지 않는다.
- 검증·진단 로직은 SQL 패턴에 맞춰 정해 둔 응답을 돌려주는 스텁으로 확인했다. 실제 AWS의 오류 문구와 SQL 문법은
  B 섹션에서 확인해야 한다.

## B. 컨테이너 스모크 테스트 (로컬 Docker, AWS 없음)
- arm64 이미지 빌드 성공. `pmcp serve --transport http`를 cloud 모드와 메모리 저장소로 실행했다.
- `/healthz` → 200
- 토큰 없이 `POST /mcp` → **401**
- 유효한 JWT로 `initialize` → 200, `tools/list` → **19개**

## C. 실제 AWS 검증 (계정 608420805531, ap-southeast-2)

### 진행 기록
| 시각(UTC) | 단계 | 결과 |
|---|---|---|
| 2026-10-25 03:5x | ACM 인증서 2개 요청 | 요청됨. DNS 검증 대기 |
| 03:57 | ECR 스택과 이미지 `pmcp:20261025035755` 푸시 | 성공 |
| 04:0x | 플랫폼 스택 1차 배포 | **실패**: 인증서가 아직 `PENDING_VALIDATION`이라 CloudFront가 거부. 롤백 완료. 남은 빈 테이블과 스택은 정리함. 원인: 인증서 대기 명령의 실패 코드를 `tail` 파이프가 가렸음. 대책: `platform.sh platform`이 인증서 `ISSUED`를 확인한 뒤에만 배포 |
| 04:04 | 원본 버킷 `pmcp-e2e-src-608420805531`에 합성 주문 업로드 | 1,500건(살아 있는 주문 1,455건, 삭제 45건), 배치당 중복 5건 포함. 정답 요약: `ground-truth/orders.json` |
| 04:44 | ACM 인증서 2개 ISSUED | 가비아 SOA 부정 캐시(86400초) 때문에 지연 |
| 04:5x | 플랫폼 배포 | `CREATE_COMPLETE`. CloudFront → ALB(오리진 비밀값) → Fargate. `/healthz` 200, 토큰 없는 `/mcp` 401, `/login` → Cognito(PKCE) |
| 05:0x | 사용자 Cognito 가입, 테넌트 온보딩 스택 | `assume_role` 모드로 연결. 이후 모든 AWS 접근은 ExternalId로 역할을 위임받아 수행 |
| 05:07, 05:17, 05:2x | 브라우저 승인 3회 | 승인 기록: channel=web, 사용자 이메일, digest 일치 |
| 05:35 | 시나리오 2 검증 | verified. 아래 표 참조 |

### 실제 AWS에서 발견하고 고친 결함 (모의 테스트로는 잡히지 않음)
| # | 증상 | 원인 | 조치 | 회귀 테스트 |
|---|---|---|---|---|
| 1 | preview에서 CloudFormation ValidationError | `raw_lines` 테이블 `field.delim`의 제어 문자를 CFN이 거부 | 기본 구분자 사용 | 템플릿 제어 문자 검사 |
| 2 | 새 스택 preview가 FORBIDDEN | `REVIEW_IN_PROGRESS` 스택은 태그를 돌려주지 않음(moto는 돌려줌) | 대기 중인 ChangeSet 태그로 소유 확인 | lifecycle 테스트 |
| 3 | 같은 검증 오류를 Codex가 재시도 | AWS ValidationError를 retryable로 분류 | `INVALID_INPUT`(retryable=false) | — |
| 4 | apply의 ChangeSet 실행 실패 | `ClientRequestToken`에 `_` 사용 불가 | 허용 문자로 변환 | 토큰 형식 테스트 |
| 5 | Iceberg CREATE TABLE 실패 | 큰따옴표 DDL은 Athena가 Trino로 파싱 | DDL은 역따옴표(Hive) | DDL 따옴표 테스트 |
| 6 | 격리 후 재실행도 실패 | `"$path" NOT IN (SELECT …)`는 파일을 연 뒤에 거름 | 격리 경로를 상수 `"$path" <> '…'`로 컴파일. Athena 실험으로 확인(상수 조건만 파일을 열지 않음) | 상수 조건 테스트 |
| 7 | 온보딩 스택 Early Validation 실패 | artifacts 버킷을 템플릿과 pmcp가 둘 다 생성 | 온보딩 템플릿에서 제거 | — |
| 8 | 매 계획마다 모든 리소스가 modify로 표시 | 계획 ID를 스택 태그로 붙임 | 계획 ID 태그 제거 | — |
| 9 | 복구 후 검증 중 진행률 항목만 unverified | 진행률 쿼리가 격리 조건 없이 원본 테이블을 직접 읽음 | 파이프라인과 같은 원본 뷰 사용 | 진행률 격리 테스트 |
| 10 | 정리(teardown) 중 흐름 스택 삭제 실패 | `pmcp-cfn-exec`가 `glue:DeleteDatabase`의 `userDefinedFunction/<db>/*` 리소스 권한이 없었음 | 온보딩 템플릿에 해당 ARN 추가 | — |

### 시나리오 결과
(실행한 뒤에만 채운다. 비어 있는 항목은 아직 실제로 검증하지 않은 것이다.)

| # | 시나리오 | 실제 AWS 결과 | 증거 |
|---|---|---|---|
| 1 | 조사 → 후보 비교 → 계획 | **통과** (클라우드, Codex). 도구 17회 호출에 오류 0. Athena 선택(월 약 1.06달러), Glue는 비용 초과(월 약 139.30달러)로 배제, 스트리밍·Redshift는 배포 불가로 표시 | `codex-cloud/s1.jsonl` |
| 2 | 생성 → 실행 → 목적지 검증 | **verified**. 출력 1,455행 = 정답 1,455건. 중복 키 0, 삭제가 남은 주문 0, 최신성 55초, 소비자 역할로 실제 읽기 성공. 운영자가 Athena로 따로 세어도 1,455 / 1,455 / 0 | 작업 `op_1m5rrykkbc4xgssexbq` |
| 3 | 규칙 수정 → 영향 → 재처리 → 검증 | 미실행 | |
| 4 | 장애 → 증거 → 진단 → 복구 → 재검증 | **verified** (2번째 시도). 깨진 파일을 넣자 05:49 실행이 실패했다. Codex(주입 사실 모름)가 진단과 심층 조사로 정확한 파일을 특정했고, 05:20 실패와는 시간상 무관하다고 구분했다. 1차 복구는 결함 6 때문에 실패했고, 작업이 실패로 기록돼 장애도 열린 채 남았다(거짓 성공 없음). 수정 후 2차 복구: 설정 변경 → 7일 재실행(8구간 모두 성공) → 재검증. 검증 쿼리 결함 9를 고친 뒤 12개 항목 모두 통과. 다음 예약 실행(06:49:54) 성공 | 작업 `op_1m5rw2qn3z5q6dceywq`, `op_1m5rx2gz251775qrgt3`, 장애 `in_5cdc…` resolved |
| 5 | 요구사항 변경 시 선택 근거 변화 | 모의만 | `tests/test_design.py` |
| 6 | 지원하지 않는 구성을 배포 가능하다고 하지 않음 | 모의만 | `tests/test_design.py` |
| 7 | 잘못된 계획·오래된 상태·중복 실행 | 모의만 | `tests/test_lifecycle.py` |

### 도구 사용성 평가 (Codex CLI를 MCP 클라이언트로 사용)
(실행한 뒤에만 채운다. 항목: 올바른 도구 선택, 인자 오류, 불필요한 호출, 전체 작업 완료 여부)
