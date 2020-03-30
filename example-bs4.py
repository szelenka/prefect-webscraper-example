import typing as T
import datetime
import requests
from bs4 import BeautifulSoup
from prefect import task, Flow, Parameter, unmapped
from prefect.engine import cache_validators
from prefect.engine.result_handlers import LocalResultHandler
from prefect.environments.storage import Docker
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
import sqlalchemy as sa


@task(
    name="Create DB",
    tags=['db']
)
def create_db(filename: T.Union[str, Parameter]) -> sa.Table:
    """
    Specify the Schema of the output table
    """
    meta = sa.MetaData(
        bind=sa.create_engine(f"sqlite:///{filename}")
    )
    tbl = sa.Table(
        'XFILES',
        meta,
        sa.Column(
            'EPISODE',
            sa.UnicodeText
        ),
        sa.Column(
            'CHARACTER',
            sa.UnicodeText
        ),
        sa.Column(
            'TEXT',
            sa.UnicodeText
        )
    )
    tbl.create(checkfirst=True)
    return tbl


@task
def insert_episode(episode: T.Tuple, tbl: sa.Table):
    """
    Insert the data into the Database
    """
    title, dialogue = episode
    values = [
        (title, *row)
        for row in dialogue
    ]
    stmt = tbl.insert().values(values)
    with tbl.bind.begin() as conn:
        rp = conn.execute(stmt)

    return


@task
def create_episode_list(base_url, main_html, bypass):
    """
    Given the main page html, creates a list of episode URLs
    """

    if bypass:
        return [base_url]

    main_page = BeautifulSoup(main_html, 'html.parser')

    episodes = []
    for link in main_page.find_all('a'):
        url = link.get('href')
        if 'transcrp/scrp' in (url or ''):
            episodes.append(base_url + url)

    return episodes


@task(
    # max_retries=3,
    # retry_delay=datetime.timedelta(minutes=5),
    # cache_for=datetime.timedelta(minutes=10),
    # cache_validator=cache_validators.all_inputs,
    tags=["web"]
)
def retrieve_url(url):
    """
    Given a URL (string), retrieves html and
    returns the html as a string.
    """

    html = requests.get(url)
    if html.ok:
        return html.text
    else:
        raise ValueError("{} could not be retrieved.".format(url))


@task
def scrape_dialogue(episode_html):
    """
    Given a string of html representing an episode page,
    returns a tuple of (title, [(character, text)]) of the
    dialogue from that episode
    """

    episode = BeautifulSoup(episode_html, 'html.parser')

    title = episode.title.text.rstrip(' *').replace("'", "''")
    convos = episode.find_all('b') or episode.find_all('span', {'class': 'char'})
    dialogue = []
    for item in convos:
        who = item.text.rstrip(': ').rstrip(' *').replace("'", "''")
        what = str(item.next_sibling).rstrip(' *').replace("'", "''")
        dialogue.append((who, what))
    return (title, dialogue)


with Flow(
        name="xfiles",
        schedule=Schedule(
            clocks=[
                # TODO: specify the schedule you want this to run, and with what parameters
                #  https://docs.prefect.io/core/concepts/schedules.html
                CronClock(
                    cron='0 0 * * *',
                    parameter_defaults=dict(
                        url='http://www.insidethex.co.uk/'
                    )
                ),
            ]
        ),
        storage=Docker(
            # TODO: change to your docker registry:
            #  https://docs.prefect.io/cloud/recipes/configuring_storage.html
            registry_url='szelenka',
            # TODO: 'pin' the exact versions you used on your development machine
            python_dependencies=[
                'requests==2.23.0',
                'beautifulsoup4==4.8.2',
                'sqlalchemy==1.3.15'
            ],
        ),
        # TODO: specify how you want to handle results
        #  https://docs.prefect.io/core/concepts/results.html#results-and-result-handlers
        result_handler=LocalResultHandler()
) as flow:
    _url = Parameter("url", default='http://www.insidethex.co.uk/')
    _bypass = Parameter("bypass", default=False, required=False)
    _db_file = Parameter("db_file", default='xfiles_db.sqlite', required=False)

    # scrape the website
    _home_page = retrieve_url(_url)
    _episodes = create_episode_list(
        base_url=_url,
        main_html=_home_page,
        bypass=_bypass
    )
    _episode = retrieve_url.map(_episodes)
    _dialogue = scrape_dialogue.map(_episode)

    # insert into SQLite table
    _db = create_db(
        filename=_db_file
    )
    _final = insert_episode.map(
        episode=_dialogue,
        tbl=unmapped(_db)
    )


if __name__ == '__main__':
    # debug the local execution of the flow
    import sys
    import argparse
    from prefect.utilities.debug import raise_on_exception

    # get any CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--visualize', required=False, default=False)
    parser.add_argument('--deploy', required=False, default=False)
    p = parser.parse_args(sys.argv[1:])

    if p.visualize:
        # view the DAG
        flow.visualize()

    # execute the Flow manually, not on the schedule
    with raise_on_exception():
        if p.deploy:
            # TODO: hack for https://github.com/PrefectHQ/prefect/issues/2165
            flow.result_handler.dir = '/root/.prefect/results'
            flow.register(
                # TODO: specify the project_name on Prefect Cloud you're authenticated to
                project_name="Sample Project Name",
                build=True,
                # TODO: specify any labels for Agents
                labels=[
                    'sample-label'
                ]
            )
        else:
            flow.run(
                parameters=dict(
                    url='http://www.insidethex.co.uk/'
                ),
                run_on_schedule=False
            )
