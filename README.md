# Review paper downloads - climsight

[Climsight](https://github.com/CliDyn/climsight) is a next-generation climate information system that uses large language models (LLMs) alongside high-resolution climate model data, scientific literature, and diverse databases to deliver accurate, localized, and context-aware climate assessments. 

This is a small repository containing code necessary to download approximately 50 thousand abstracts and 30 thousand full text pdfs for the climsight system.

The source of the paper repository is OpenAlex. However, other sites were queried to obtain the full texts (openalex, unpaywall, semantic scholar, elsevier api).

Using the OpenAlex "Topics" categorization, each review paper has been categorized by the theme which it pertains to. The categories can be found by inspecting the `openalex_ess_topics.csv` file. Each downloaded abstract also contains a Topic ID.

If you would like to run the code for yourself, please note that the API key for elsevier, and your email address for API registrations must be input in the `API_KEYS.txt` file in the git root directory.


# Data Collection Methodology
The abstracts were filtered according to the following methodology:

1. The OpenAlex Topics spreadsheet was manually inspected, and the list of topics broadly included in earth systems sciences were selected. You may inspect these topics by taking a look at the `openalex_ess_topics.csv` file in the root directory. Out of 4000 OpenAlex topics, 600 were selected for inclusion according to the earth system science criteria.
   
3. The OpenAlex "search" api tool was used to filter all papers with these topic ID's as well as restricting results to papers with titles containing the term "review", while NOT containing the term "peer review".

4. A script was run to download all relevant abstracts, and the corresponding review papers in pdf format from OpenAlex.

# Running the code

`pandas` and `tqdm` installation will be necessary, along with some other standard python packages.

A. (Optional) To view the counts of all papers in openalex matching the search specified above, navigate to `src/` and run:

	python A_print_counts_of_all_papers_matching_search.py


B. Download all the abstracts for the topics.

	python B_download_all_topics.py
 

C. Combine the abstract csv files which were saved in separate folder for each topic into a single `all_records.csv` file.

	python C_combine_csvs.py


D. Download the full texts of all matching abstracts (can take 1-3 days to download 50,000 files).

	python D_download_fulltexts.py


# Tests

Tests can be run from the git root directory with:
```
python tests/test_run_all.py
```
