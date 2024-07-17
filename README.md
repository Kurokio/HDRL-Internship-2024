# Building Tools to Analyze FAIR for HDRL Datasets
> This project aims to aid in analyzing the FAIR guidelines (Findable, Accessible, Interoperable, and Reusable) for the NASA SPASE records in the NumericalData and DisplayData categories.

This project consists of a host of scripts and Jupyter notebooks which perform the following:
- scrapes SPASE records for fields useful for analyzing FAIR
- converts desired SPASE record fields into a SQLite database.
- conducts analysis tests using queries on database
- determines a "FAIR score" of all records
- reports these findings in the form of created charts

*Note that this project was tested in Summer 2024 on SPASE version 2.6.1*

## Prerequisites
1. Clone the NASA SPASE repository found at https://github.com/hpde/NASA/tree/master by running the following.
```python
git clone -b master --single-branch --depth=1 https://github.com/hpde/NASA
```
2. Clone this repo.
```python
git clone https://github.com/Kurokio/HDRL-Internship-2024
```

## Usage
Follow the notebook "HowToUse" which walks you through step-by-step how to do the actions listed above. 

For more information on adding additional fields to scrape and add to the database for analysis, refer to the "HowToAdd" notebook.

## Contribution
Contributors and collaborators are welcome. Acceptable contributions can be documentation, code, suggesting ideas, and submitting issues and bugs.

Make sure to be nice when contributing and submitting commit messages, as this is a project developed by an intern that is still learning.

## Credits
Thanks to the following people who helped make this project a reality:
- <a href="https://github.com/rebeccaringuette" target="_blank">@rebeccaringuette</a>

## Contact
Zach Boquet - <a href="https://www.linkedin.com/in/zach-boquet-62a996254/" target="_blank">LinkedIn</a> - <a href="https://orcid.org/0009-0005-1686-262X" target="_blank">ORCiD</a>

## License
This project uses the following license: <a href="https://github.com/Kurokio/HDRL-Internship-2024/blob/main/LICENSE" target="_blank">Apache-2.0</a>
