# 📱 주식 텔레그램 알림 봇

## 기능
- 매일 오전 7:30 – 나스닥/S&P500 변동률 + Fear&Greed 지수 + 섹터별 변동률
- 실시간 (10분마다) – DART 주요 기관 지분 변동 알림
- 실시간 (30분마다) – KITA 메모리 반도체 수출 데이터 업데이트 알림

---

## Railway 배포 방법

### 1단계: GitHub에 코드 올리기
1. [github.com](https://github.com) 로그인
2. 오른쪽 위 **"+"** 버튼 → **"New repository"** 클릭
3. Repository name: `stock-bot` 입력
4. **"Create repository"** 클릭
5. 화면에서 **"uploading an existing file"** 클릭
6. `main.py`, `requirements.txt`, `README.md` 파일 3개를 드래그앤드롭
7. **"Commit changes"** 클릭

### 2단계: Railway에서 배포
1. [railway.app](https://railway.app) 로그인
2. **"New Project"** 클릭
3. **"Deploy from GitHub repo"** 클릭
4. `stock-bot` 선택
5. 배포 시작됨

### 3단계: 환경변수 설정 (중요!)
1. Railway 프로젝트 화면에서 **"Variables"** 탭 클릭
2. 아래 3개를 각각 추가:

| 변수명 | 값 |
|--------|-----|
| `TELEGRAM_TOKEN` | BotFather에서 받은 토큰 |
| `TELEGRAM_CHAT_ID` | 본인 Chat ID (숫자) |
| `DART_API_KEY` | DART에서 발급받은 API 키 |

3. 저장하면 자동으로 재시작되며 봇이 실행됩니다! ✅
