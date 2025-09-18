# Cymphony

[//]: # (![Build Status]&#40;https://img.shields.io/badge/build-passing-brightgreen&#41;)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)


Cymphony is a novel software system that uses human-in-the-loop crowdsourcing to help data scientists solve complex data cleaning and integration tasks.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Overview

Nowadays, computer science research is shifting from publishing theoretical papers to building practical tools that can be used in the real world. This shift is driven by the realization that contributions to CS research are increasingly dominated by the industrial community—thanks to their access to widely used tools—while academic researchers often lack similar resources.

Crowdsourcing—the process of obtaining data labels from a large pool of online users—is central to both industry and academia. Companies like Google and Amazon leverage proprietary tools to collect crowdsourced data, yet for academia, crowdsourcing remains something of a black box. This is a significant challenge because crowdsourced data forms the backbone for training machine learning (ML) and artificial intelligence (AI) algorithms.

Cymphony bridges this gap by providing a platform that enables researchers and data scientists to collect high-quality data labels from online workers. Moreover, it extends traditional crowdsourcing tools by:

1. **Leveraging In-House Expertise:**  
   Allowing teams of in-house workers to verify and label data directly within the platform.

2. **Chaining Human and Automated Operations:**  
   Integrating human-generated outputs with SQL operations to perform complex tasks such as information extraction and fuzzy entity matching.

3. **Integrating with Established Crowdsourcing Tools:**  
   Seamlessly connecting with platforms like Amazon Mechanical Turk (AMT) to outsource labeling tasks when needed.

By incorporating Cymphony into your data science pipeline, you can generate high-quality training data from the start, use active learning during model training, or improve prediction accuracy by having humans correct data that ML/AI models misclassify.

### Key Features

- **Hybrid Workflows:**  
  Combine SQL operators with human operators to perform data labeling, extraction, classification, or matching.

- **Flexible Human Operators:**  
  Utilize online crowds (e.g., Amazon Mechanical Turk) or in-house domain experts to suit your project needs.

- **Chained Workflows:**  
  Build complex data processing pipelines by chaining multiple workflows together with custom code via built-in web APIs.

- **Versatile Use Cases:**  
  - **Dataset Generation & Cleaning:**  
    Create new datasets or clean existing ones by labeling and classifying data.
  - **Entity & Schema Matching:**  
    Detect duplicates, match similar products, and infer potential foreign key relationships.
  - **Data Integration:**  
    Merge data from disparate sources into unified data lakes.

## Download and Installation

### [![Setup Instructions](https://img.shields.io/badge/Setup-Instructions-blue)](./setup_instructions.md)

### Dependencies

The required dependencies are:
* django (python based web framework). Tested on version 3.1.5 and 3.2.
* django_registration (provides user registration functionality). Tested on version 3.1.1 and 3.2.
* python (programming language). Tesed on version 3.8.5 and 3.8.13.
* psycopg2-binary (python postgresql database adapter). Tesed on version 2.9.3.
* requests (provides HTTP client functionality that we use for simulating synthetic workers). Tested on version 2.25.1 and 2.27.1.
* boto3 (aws sdk for python that we use for integrating with amazon mechanical turk - a.m.t.). Tested on version 1.17.109 and 1.20.0.
* cryptography (provdes cryptographic primitives that we use for encrypting user's amt credentials if they choose to use the amt operator within the workflow). Tested on version 3.4.7 and 3.4.8.
* gunicorn (python wsgi http server for unix that we use for deploying the system). Tested on version 20.1.0.
* postgresql (advanced relational dbms that provides support for storage, querying, functions, and concurrency control). Tested on version 12.11.

Some additional (optional) tools were used for development and maintainence of this system:
* pgadmin4 (provides administration interface for postgresql). Used version 4.21 and 6.19.
* pycharm (provides development environment and tools for programming professional projects in python). Used version 2020.1.2.

### Platforms
* Cymphony deployment has been tested on Linux.
* Cymphony can be accessed via gui based interface, or via web apis.
  - Access to gui interface has been tested on google chrome (version 115) and mozilla firefox (version 115), from microsoft windows 10 and ubuntu 18.04 each.
  - Access to web apis has been tested via the python based requests library version 2.25.1 and 2.27.1.
* Cymphony can be accessed by users who can act either as requesters or as workers.
  - Requesters are data engineers and scientists who wish to create and publish workflows.
  - Workers are in-house people (likely domain experts) who label data to drive the published workflows to completion. (These are not to be confused with amt workers at large who do not access cymphony directly).
* Additional remarks:
  - Requesters and in-house workers both can use cymphony via the gui to publish and work on workflows respectively.
  - Requsters can use cymphony via web apis to publish workflows as part of larger custom code. For example, requester can have some active learning code wherein they may need to start a workflow for each iteration.
  - Synthetic workers can be simulated via web apis so as to test/debug workflows.

## Standalone Usage

Here are some commands to get started:

### Running the Development Server

Start the Django development server:

```bash
python manage.py runserver 0.0.0.0:8000
 ```
This will start the server on port 8000. You can access the application by navigating to `http://localhost:8000` in your web browser.

### Running Background Tasks

Start the task monitor daemon with a configurable interval (in seconds):
```bash
python manage.py monitor_tasks --interval 60
```
This command will continuously monitor and process abandoned tasks at the specified interval.

<!-- ### Usage Example -->

## Using Cymphony as a component in your system
Cymphony can be integrated as a component in your system by incorporating it into your existing data processing pipeline. 

### Admin Setup
Before letting your users sign up, follow the steps in the link below to configure the admin interface, roles, and special accounts:  
[![Admin Setup](https://img.shields.io/badge/Admin-Setup-red)](./admin_setup.md)

### Creating Workflows using APIs
You can use the provided APIs to create workflows and manage runs:  
[![API Docs](https://img.shields.io/badge/API-Docs-blue)](./api-docs/api-endpoints.md)

<!-- 2. To specify workflows, here is the language. -->

### Having Workers Annotate Data
Then, you can have workers who are either stewards or general users, come in and annotate the data in your jobs.
<!-- Example here -->

## Contributing

### Exploring the Code
To dive into the project, start by exploring controller/views.py as an entry point to the main application logic.

## License

This project is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
