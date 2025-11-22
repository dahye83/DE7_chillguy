## Airflow - Docker 환경 구축

1. **프로젝트 디렉터리 생성**
    - 적절한 경로에 폴더를 생성한다.
    - 생성한 폴더에 깃허브 디렉터리를 복사한다. `git clone https://github.com/dahye83/DE7_chillguy`
    - 깃허브 디렉터리 폴더로 이동한다. `cd DE7_chillguy`

2. **Airflow 환경 변수 설정**
    - AWS S3, Snowflake, Slack App을 설정한다.
    - `.env` 파일을 생성한다.
    - Airflow에서 사용되는 모든 변수를 입력한다.
    ```bash
    # Connection: AIRFLOW_CONN_[커넥션 이름]
    AIRFLOW_CONN_S3_CONN_ID="aws://KEY:SECRET@region"
    AIRFLOW_CONN_SNOWFLAKE_CONN_ID="snowflake://USER:PASS@ACCOUNT/?warehouse=WH&role=ROLE"

    # Variable: AIRFLOW_VAR_[변수 이름]
    AIRFLOW_VAR_BUCKET_NAME="value"         # S3 Bucket 이름
    AIRFLOW_VAR_SLACK_ALERT_URL="value"     # 슬랙으로 알림을 받을 앱 Hook URL
    AIRFLOW_VAR_SERVICEKEY="value"          # 데이터를 받을 API Key
    ```

3. **Docker**
    - Docker를 실행한다.
    - 프로젝트 루트 폴더에서 명령 프롬프트를 실행한다.
    ```bash
    docker compose build    # 도커 이미지 빌드
    docker compose up -d    # 도커 컨테이너 실행
    docker compose ps       # 실행 상태 확인
    ```
    - 실행 상태를 확인하였을 때 airflow-worker / airflow-scheduler / airflow-webserver / airflow-triggerer 가 실행중인지 확인한다.

4. **Airflow Web UI**
    - `http://localhost:8080/`에 접속한다.
    - `ID: airflow, Password: airflow`로 로그인한다.
    ![](image/airflow_main.png)
    - 만약 `.env` 파일에 변수를 입력하지 않았거나, DAG에 오류가 있다면 상단 탭 아래에 오류가 표시된다.
    ![](image/airflow_error.png)

5. **DAG, plugin 파일 이동**
    - 프로젝트의 airflow/dags/하위 경로에 있는 파일들과 plugins 폴더를 루트 폴더에 생성된 dags/ 경로로 이동한다.
