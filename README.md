# prefect-webscraper-example

This repository is a `complete` tutorial of how to use [Prefect](https://docs.prefect.io/core/) to scrape a website, while 
also deploying it to Prefect Cloud for scheduled orchestration.

This follows mostly the tutorial documented here, but written to run on Prefect Cloud on a schedule:
- https://docs.prefect.io/core/advanced_tutorials/advanced-mapping.html#outline

Following that example, it's writing data to a local SQLite table, which doesn't make much sense when our
images are ephemeral, but it should illustrate the pipeline execution. In practice, when orchestrating through
Prefect Cloud, we'd likely want to preserve the data to some database or repository that resides on a different 
dedicated system.

## Installation

You'll need a Python environment with the following packages installed:

```bash
pip install -r requirements.txt
```

IF you want to visualize the DAG, you'll need `graphviz` installed. This can be done with one command if you're using 
conda:
```bash
conda install graphviz
```

## Examples

### BeautifulSoup

The example on Prefect's site leverages the `requests` library, along with `beautifulsoup4`. This pattern works for basic
websites that don't involve a lot of JavaScript manipulation of the DOM.

A working example of using BeautifulSoup to parse a website on a schedule in Prefect Cloud is found in:
- [example-bs4.py](./example=bs4.py)

### Selenium

For more modern websites that use a lot of AJAX with JavaScript DOM manipulation, you'll need to simulate execution of 
the JavaScript, and parse the page as it would load in a traditional browser. For this, there are headless versions of
popular web browsers, that allow you to parse it with similar CSS or XPATH syntax.

A working example of using Selenium to parse a website on a schedule in Prefect Cloud is found in:
- [example-selenium.py](./example-selenium.py)

#### Selenium Drivers

To leverage Selenium on your local machine, you'll need to download the appropriate driver from their website:
- https://selenium-python.readthedocs.io/installation.html#drivers

In this example, we're using the `chromedriver` located in the same directory as this code. 

When deploying to Prefect Cloud, the reference code will take hints from the official [selenium chrome image](https://github.com/SeleniumHQ/docker-selenium) as a base,
then add the Prefect Flow code for the final image that's orchestrated.

This can be viewed in the [Dockerfile](./Dockerfile) file.

## Project Layout

TYPE|OBJECT|DESCRIPTION
---|---|---
📁|[docker](./docker)|Non-source code related files used by the [Dockerfile](./Dockerfile) during the build process
📄|[Dockerfile](./Dockerfile)|Dockerfile to build a base image for the selenium chrome driver
📄|[example-bs4.py](./example=bs4.py)|Example website scraper Prefect Flow ready for Prefect Cloud using BeautifulSoup
📄|[example-selenium.py](./example-selenium.py)|Example website scraper Prefect Flow ready for Prefect Cloud using Selenium
📄|[README.md](README.md)|This file you're reading now
📄|[requirements.txt](./requirements.txt)|Python packages required for local development of Prefect Flows in this repository

