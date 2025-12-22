<div align="center">

![Spark Analyzer Logo](docs/images/logo.svg)

# Spark Analyzer

**Apache Spark를 위한 성능 튜닝 및 로그 분석**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

*분석하고, 시각화하고, 최적화하세요.*

</div>

---

**Spark Analyzer**는 원본 Apache Spark 이벤트 로그를 실행 가능한 인사이트로 변환해줍니다. 웹 기반 대시보드를 통해 Spill, Skew, Shuffle Load와 같은 성능 병목 현상을 식별하여 데이터 엔지니어가 작업을 효율적으로 최적화할 수 있도록 돕습니다.

## ✨ 주요 기능 (Key Features)

- **📂 로그 관리 (Log Management)**: 드래그 앤 드롭 업로드, 강력한 검색 및 간편한 삭제.
- **📊 인터랙티브 대시보드**:
    - **요약 분석**: 30개 이상의 지표(Duration, CPU/Mem, Spill)로 여러 앱을 나란히 비교.
    - **상세 흐름**: SQL 시각화 및 데이터 흐름 다이어그램으로 특정 Stage 심층 분석.
- **⚡ 병목 탐지**: **Max Spill** Stage 및 **Data Skew**와 같은 치명적인 문제를 자동으로 강조.
- **🛠️ 파워 툴**: CSV 내보내기, 단위 변환(B→TB), 동적 정렬 및 메트릭 정의.

## 🚀 빠른 시작 (Quick Start)

### 사전 요구 사항 (Prerequisites)
- **Python 3.8+** (`pyenv` 사용 권장 - 3.11)
- 최신 웹 브라우저 (Chrome, Edge, Safari)

### 설치 및 실행 (Installation & Run)
편리한 `Makefile`을 제공하여 몇 초 만에 시작할 수 있습니다.

#### 1. **Pyenv 설치**
   
   macOS에서 Homebrew를 사용하여 pyenv를 설치합니다:
   ```bash
   $ brew install pyenv pyenv-virtualenv
   ```

   설치 후 셸 설정 파일에 pyenv를 추가합니다 (`.zshrc` 또는 `.bash_profile`):
   ```bash
   $ echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
   $ echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.zshrc
   $ echo 'eval "$(pyenv init --path)"' >> ~/.zshrc
   $ echo 'eval "$(pyenv init -)"' >> ~/.zshrc
   $ echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.zshrc
   ```

   셸을 재시작하거나 설정을 다시 로드합니다:
   ```bash
   $ source ~/.zshrc
   ```

#### 2. **Python 3.11 설치**
   
   pyenv를 사용하여 Python 3.11을 설치합니다:
   ```bash
   $ pyenv install 3.11
   ```

   설치 가능한 Python 버전 확인:
   ```bash
   $ pyenv install --list | grep 3.11
   ```

#### 3. **가상환경 생성 및 활성화**
   
   Python 3.11 기반의 `spark_analyzer_env` 가상환경을 생성합니다:
   ```bash
   $ pyenv virtualenv 3.11 spark_analyzer_env
   ```

   프로젝트 디렉토리로 이동하여 가상환경을 활성화합니다:
   ```bash
   $ cd /path/to/spark_analyzer
   $ pyenv activate spark_analyzer_env
   ```

   또는 프로젝트 디렉토리에서 자동으로 활성화되도록 설정:
   ```bash
   $ pyenv local spark_analyzer_env
   ```

#### 4. **의존성 패키지 설치**
   
   가상환경이 활성화된 상태에서 `requirements.txt`의 패키지를 설치합니다:
   ```bash
   (spark_analyzer_env) $ pip install -r requirements.txt
   ```

   설치 확인:
   ```bash
   (spark_analyzer_env) $ pip list
   ```

#### 5. **애플리케이션 실행**
   `SHS_URL` 환경변수를 설정하여 Spark History Server와 연동할 수 있습니다. (기본값: `http://localhost:18080`)

   ```bash
   # 포그라운드 (개발용)
   (spark_analyzer_env) $ make run

   # 백그라운드 (서비스용)
   (spark_analyzer_env) $ make start

   # 사용자 정의 SHS URL 사용 예시
   (spark_analyzer_env) $ export SHS_URL="http://spark-history-server:18080"
   (spark_analyzer_env) $ make run
   ```

   > **SHS 연동**: 편의를 위해 환경변수가 설정되면, 분석 결과의 **Application ID**를 클릭했을 때 해당 Spark History Server 페이지로 바로 이동합니다.

#### 6. **대시보드 접속**
   브라우저에서 [http://localhost:8000](http://localhost:8000)을 엽니다.

> **참고**: 
> - 백그라운드 서비스를 중지하려면 `make stop`을 실행하세요.
> - 환경을 초기화하려면 `make clean`을 사용하세요.
> - 가상환경을 비활성화하려면 `pyenv deactivate`를 실행하세요.

## 📸 Snapshots
### Upload log files

<img src="docs/images/spark_analyzer_file_upload.gif" alt="Upload Log Files" width="100%">

### Analyze Seleted

<img src="docs/images/spark_analyzer_analyze_selected.gif" alt="Analyze Selected" width="100%">

### Create Graph

<img src="docs/images/spark_analyzer_create_graph.gif" alt="Create Graph" width="100%">

### Detail View

<img src="docs/images/spark_analyzer_detail_view.gif" alt="Detail View" width="100%">


## 📁 프로젝트 구조 (Project Structure)

```text
spark_analyzer/
├── Makefile                    # 실행 관리
├── requirements.txt            # 의존성 목록
├── web_app.py                  # FastAPI 서버
├── spark_log_parser.py         # 로그 파싱 엔진
├── spark_metric_definitions.json # 메트릭 정의
├── event_logs/                 # 로그 저장소
├── latest_analysis_result.csv  # 분석 캐시
└── static/                     # 프론트엔드 리소스
    ├── index.html              # SPA 진입점
    ├── css/style.css           # 모던 스타일링
    └── js/app.js               # 앱 로직
```
