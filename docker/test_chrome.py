import tempfile
from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver as RemoteWebDriver

options = webdriver.ChromeOptions()
options.add_argument('--headless')
# options.add_argument("--disable-extensions") # disabling extensions
# options.add_argument("--disable-gpu") # applicable to windows os only
# options.add_argument("--disable-dev-shm-usage") # overcome limited resource problems
options.add_argument("--no-sandbox") # Bypass OS security model
options.add_experimental_option(
    'prefs',
    {
        'download.default_directory': tempfile.gettempdir()
    }
)


driver = webdriver.Chrome(
    executable_path='/usr/bin/chromedriver',
    options=options
)


assert isinstance(driver, RemoteWebDriver), 'Invalid instance of driver'


url = 'http://www.cisco.com/'
driver.get(url=url)
print(driver.current_url)
