import typing as T
import datetime
from pathlib import Path
import tempfile
import time
import random
import re
import sqlalchemy as sa

from prefect import task, Flow, Parameter, unmapped
from prefect.engine.result import Result
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.engine import cache_validators
from prefect.engine.result_handlers import GCSResultHandler
from prefect.environments.storage import Docker
from prefect.utilities.logging import get_logger

from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver as RemoteWebDriver
from selenium.common.exceptions import TimeoutException, InvalidSelectorException, NoSuchElementException, ElementNotVisibleException, InvalidElementStateException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


def click_on_xpath(driver: RemoteWebDriver, xpath: str, timeout: int = 60):
    time.sleep(random.uniform(0.5, 1.))
    try:
        resolved = WebDriverWait(driver, timeout=timeout).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        resolved.click()
        return resolved
    except (TimeoutException, ) as ex:
        get_logger().error(f'Unable to locate element: {xpath} within {timeout} seconds')
        raise ex
    except (InvalidSelectorException, ) as ex:
        raise ex
    except (NoSuchElementException, ElementNotVisibleException, InvalidElementStateException, ) as ex:
        raise ex


def wait_on_visible(driver: RemoteWebDriver, xpath: str, timeout: int = 60):
    try:
        resolved = WebDriverWait(driver, timeout=timeout).until(
            EC.visibility_of_element_located((By.XPATH, xpath))
        )
        return resolved
    except (TimeoutException, ) as ex:
        get_logger().error(f'URL: {driver.current_url} unable to locate XPATH: {xpath} in timeout: {timeout}')
        raise ex
    except (InvalidSelectorException, ) as ex:
        raise ex
    except (NoSuchElementException, ElementNotVisibleException, InvalidElementStateException, ) as ex:
        get_logger().error(f'URL: {driver.current_url} unable to locate XPATH: {xpath}')
        raise ex


def get_element_text(driver: RemoteWebDriver, xpath: str, timeout: int = 60) -> T.Optional[str]:
    try:
        return wait_on_visible(driver=driver, xpath=xpath, timeout=timeout).text
    except (NoSuchElementException, ElementNotVisibleException, InvalidElementStateException, ) as ex:
        return None


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
        'REVIEWS',
        meta,
        sa.Column(
            'metascore',
            sa.Float
        ),
        sa.Column(
            'crit_reviews',
            sa.Integer
        ),
        sa.Column(
            'user_score',
            sa.Float
        ),
        sa.Column(
            'user_reviews',
            sa.Integer
        ),
        sa.Column(
            'publisher',
            sa.Unicode
        ),
        sa.Column(
            'developer',
            sa.Unicode
        ),
        sa.Column(
            'genres',
            sa.Unicode
        ),
        sa.Column(
            'rating',
            sa.Unicode
        ),
        sa.Column(
            'release_date',
            sa.DateTime
        ),
        sa.Column(
            'platform',
            sa.Unicode,
            index=True
        ),
        sa.Column(
            'source_url',
            sa.Unicode,
            nullable=False
        ),
        sa.Column(
            'scraped_on',
            sa.DateTime,
            server_default=sa.func.datetime(sa.literal_column("'now'"), sa.literal_column("'utc'"))
        )
    )
    tbl.create(checkfirst=True)
    return tbl


@task
def insert_data(data: T.Dict[str, T.Any], gaming_platform: str, tbl: T.Union[sa.Table, Result]):
    """
    Insert the data into the Database
    """
    data.update(
        platform=gaming_platform,
        scraped_on=datetime.datetime.utcnow()
    )
    stmt = tbl.insert().values(data)
    with tbl.bind.begin() as conn:
        rp = conn.execute(stmt)

    return


@task
def task_initialize_browser(
        path_to_chromedriver: T.Union[str, Parameter]
):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument("--no-sandbox")
    options.add_experimental_option(
        'prefs',
        {
            'download.default_directory': tempfile.gettempdir()
        }
    )
    driver = webdriver.Chrome(
        executable_path=path_to_chromedriver,
        options=options
    )
    assert isinstance(driver, RemoteWebDriver)
    # get_logger().info(f"Selenium service_url: {svc.service_url}")
    return driver


@task
def task_close_driver(
        driver: T.Union[RemoteWebDriver, Result]
):
    if driver:
        driver.quit()


@task(
    max_retries=3,
    retry_delay=datetime.timedelta(minutes=5),
    cache_for=datetime.timedelta(minutes=10),
    cache_validator=cache_validators.all_inputs
)
def task_locate_links_on_home_page(
        driver: T.Union[RemoteWebDriver, Result],
        url: T.Union[str, Parameter],
        gaming_platform: T.Union[str, Parameter]
) -> T.Union[T.List[str], Result]:
    # download the HTML from the site
    driver.get(url=url)

    # navigate to "Games"
    resolved = click_on_xpath(
        driver=driver,
        xpath='//span[@class="primary_nav_text" and contains(string(), "Games")]'
    )

    # navigate to selected gaming_platform
    resolved = click_on_xpath(
        driver=driver,
        xpath=f'//div[@class="column platforms"]//label[@class="mc_nav_picks" and contains(string(), "{gaming_platform}")]'
    )

    # navigate to selected gaming_platform Home page
    resolved = click_on_xpath(
        driver=driver,
        xpath=f'//div[@class="column subnav ajax"]//a[contains(string(), "{gaming_platform} Home")]'
    )

    # navigate to 'see all' list of games
    resolved = click_on_xpath(
        driver=driver,
        xpath=f'//p[@class="see_all"]/a[contains(string(), "see all")]'
    )

    # navigate to 'All Releases' list
    resolved = click_on_xpath(
        driver=driver,
        xpath='//li[contains(@class, "tab_available")]/a'
    )

    # iterate through pages to collect URL of games to collect
    def get_all_links(_driver):
        _links = list()
        titles_xpath = '//div[contains(@class, "product_title")]/a'
        wait_on_visible(driver=driver, xpath=titles_xpath)
        for elem in _driver.find_elements_by_xpath(titles_xpath):
            _links.append(elem.get_property('href'))

        return _links

    # get current page links
    links = get_all_links(_driver=driver)

    # get links from other pages
    next_page = '//li[contains(@class, "active_page")]//parent::li//following-sibling::li/a'
    while True:
        try:
            click_on_xpath(
                driver=driver,
                xpath=next_page,
                timeout=5
            )
            links += get_all_links(_driver=driver)
        except (TimeoutException, NoSuchElementException, ):
            break

    get_logger().info(f"Discovered {len(links)} links to follow")
    return links


@task
def task_filter_links(
        links: T.Union[T.List[str], Result],
        gaming_platform: T.Union[str, Parameter],
        tbl: T.Union[sa.Table, Result]
) -> T.Union[T.List[str], Result]:
    """
    Remove any links which we have 'recently' scraped
    """
    stmt = sa.select([
        tbl.c.source_url
    ]).where(sa.and_(
        tbl.c.platform == gaming_platform,
        tbl.c.source_url.in_(links),
        # tbl.c.scraped_on > datetime.datetime.utcnow() - datetime.timedelta(days=1)
    ))
    rp = tbl.bind.execute(stmt)
    results = set([_[0] for _ in rp.fetchall()])
    output = list(set(links).difference(results))
    get_logger().info(f'Discovered {len(output)} links to parse')
    return output


@task(
    max_retries=3,
    retry_delay=datetime.timedelta(minutes=5),
    cache_for=datetime.timedelta(minutes=10),
    cache_validator=cache_validators.all_inputs
)
def task_extract_data_from_game_page(
        url: T.Union[str, Parameter],
        driver: T.Union[RemoteWebDriver, Result],
) -> T.Union[T.Dict[str, T.Any], Result]:
    driver.get(url=url)
    try:
        metascore = float(get_element_text(driver=driver, xpath='//div[contains(@class, "metascore_w")]/span'))
    except ValueError as ex:
        metascore = None

    try:
        crit_reviews = int(get_element_text(
            driver=driver,
            xpath='//div[contains(@class, "metascore_w")]//span[contains(@class, "count")]/a/span'
        ))
    except ValueError as ex:
        crit_reviews = None

    try:
        user_score = float(get_element_text(driver=driver, xpath='//div[contains(@class, "userscore_wrap")]/a/div'))
    except ValueError as ex:
        user_score = None

    try:
        user_reviews = int(re.sub(r'[^\d]', '', get_element_text(
            driver=driver,
            xpath='//div[contains(@class, "userscore_wrap")]//span[contains(@class, "count")]'
        )))
    except ValueError as ex:
        user_reviews = None

    try:
        publisher = get_element_text(
            driver=driver,
            xpath='//div[contains(@class, "product_data")]//li[contains(@class, "publisher")]/span[contains(@class, "data")]'
        )
    except (NoSuchElementException, ) as ex:
        publisher = None

    try:
        developer = get_element_text(
            driver=driver,
            xpath='//div[contains(@class, "product_details")]//li[contains(@class, "developer")]/span[contains(@class, "data")]'
        )
    except (NoSuchElementException, ) as ex:
        developer = None

    try:
        genres = '|'.join([_.text for _ in driver.find_elements_by_xpath('//div[contains(@class, "product_details")]//li[contains(@class, "product_genre")]/span[contains(@class, "data")]')])
    except (NoSuchElementException, ) as ex:
        genres = None


    try:
        rating = get_element_text(
            driver=driver,
            xpath='//div[contains(@class, "product_details")]//li[contains(@class, "product_rating")]/span[contains(@class, "data")]'
        )
    except (NoSuchElementException, ) as ex:
        rating = None

    try:
        release_date = datetime.datetime.strptime(get_element_text(
            driver=driver,
            xpath='//div[contains(@class, "product_data")]//li[contains(@class, "release_data")]/span[contains(@class, "data")]'
        ), '%b %d, %Y')
    except ValueError as ex:
        release_date = None

    data = dict(
        metascore=metascore,
        crit_reviews=crit_reviews,
        user_score=user_score,
        user_reviews=user_reviews,
        publisher=publisher,
        developer=developer,
        genres=genres,
        rating=rating,
        release_date=release_date,
        source_url=url
    )
    return data


with Flow(
        name="example-selenium",
        schedule=Schedule(
            clocks=[
                # TODO: specify the schedule you want this to run, and with what parameters
                #  https://docs.prefect.io/core/concepts/schedules.html
                CronClock(
                    cron='0 0 * * *',
                    parameter_defaults=dict(
                        home_page='https://www.metacritic.com/',
                        gaming_platform='Switch'
                    )
                ),
            ]
        ),
        storage=Docker(
            # TODO: change to your docker registry:
            #  https://docs.prefect.io/cloud/recipes/configuring_storage.html
            registry_url='szelenka',
            # TODO: need to specify a base Docker image which has the chromedriver dependencies already installed
            base_image='szelenka/python-selenium-chromium:3.7.4',
            # TODO: 'pin' the exact versions you used on your development machine
            python_dependencies=[
                'selenium==3.141.0',
                'sqlalchemy==1.3.15'
            ],
        ),
        # TODO: specify how you want to handle results
        #  https://docs.prefect.io/core/concepts/results.html#results-and-result-handlers
        result_handler=GCSResultHandler(
            bucket='prefect_results'
        )
) as flow:
    # specify the DAG input parameters
    _path_to_chromedriver = Parameter('path_to_chromedriver', default='/usr/bin/chromedriver')
    _home_page_url = Parameter('home_page', default='https://www.metacritic.com/')
    _gaming_platform = Parameter('gaming_platform', default='Switch')
    _db_file = Parameter("db_file", default='game_reviews.sqlite', required=False)

    # specify function flow for DAG
    _driver = task_initialize_browser(
        path_to_chromedriver=_path_to_chromedriver
    )

    # extract links of pages to parse
    links_from_home_page = task_locate_links_on_home_page(
        driver=_driver,
        url=_home_page_url,
        gaming_platform=_gaming_platform
    )

    _db = create_db(
        filename=_db_file
    )
    _filtered_links = task_filter_links(
        gaming_platform=_gaming_platform,
        links=links_from_home_page,
        tbl=_db
    )

    # parse data off the pages
    _raw_data = task_extract_data_from_game_page.map(
        url=_filtered_links,
        driver=unmapped(_driver)
    )
    task_close_driver(
        driver=_driver
    ).set_upstream(_raw_data)

    # insert into SQLite table
    _final = insert_data.map(
        data=_raw_data,
        gaming_platform=unmapped(_gaming_platform),
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
                    path_to_chromedriver=Path('./chromedriver').absolute().as_posix()
                ),
                run_on_schedule=False
            )
