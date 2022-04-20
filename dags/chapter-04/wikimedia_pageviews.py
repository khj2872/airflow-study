import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from urllib import request

from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(
    dag_id="wikimedia_pageviews",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly",
    template_searchpath="/tmp"
)


# get_data = BashOperator(
#     task_id="get_data",
#     bash_command=(
#         "curl -o /tmp/wikipageviews.gz "
#         "https://dumps.wikimedia.org/other/pageviews/"
#         "{{ execution_date.year }}/"
#         "{{ execution_date.year }}-{{ '{:02}'.format(execution_date.month) }}/"
#         "pageviews-{{ execution_date.year }}"
#         "{{ '{:02}'.format(execution_date.month) }}"
#         "{{ '{:02}'.format(execution_date.day) }}-"
#         "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
#     ),
#     dag=dag,
# )

def _get_data(execution_date, output_path):
    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    # output_path="/tmp/wikipageviews.gz"
    print(f"output_path: {output_path}")
    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={"output_path": "/tmp/wikipageviews"},
    dag=dag
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force /tmp/wikipageviews.gz",
    dag=dag
)


def _fetch_pageviews(pagenames, execution_date, **_):
    result = dict.fromkeys(pagenames, 0)
    with open(f"/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    # print(result)

    with open(f"/tmp/postgres_query.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": {
            "Google",
            "Amazon",
            "Microsoft",
            "Facaebook",
        }
    },
    dag=dag
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="my_postgres",
    sql="postgres_query.sql",
    dag=dag
)

get_data >> extract_gz >> fetch_pageviews >> write_to_postgres
