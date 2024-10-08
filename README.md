# Building Tools to Analyze FAIR for HDRL Datasets - [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.13287869.svg)](https://doi.org/10.5281/zenodo.13287869)

> This project aims to aid in analyzing the FAIR guidelines (Findable, Accessible, Interoperable, and Reusable) for the NASA SPASE records in the NumericalData and DisplayData categories.

This project consists of a host of scripts and Jupyter notebooks which perform the following:
- scrapes SPASE records for fields useful for analyzing FAIR
- converts desired SPASE record fields into a SQLite database.
- conducts analysis tests using queries on database
- determines a "FAIR score" of all records
- reports these findings in the form of created charts

Also included is a script and notebook showing how to check the data availability of the HAPI data access links found in the NASA SPASE records.

*Note that this project was tested in Summer 2024 on SPASE version 2.6.1*

## Installation Instructions
1. Clone this repo.
```python
git clone https://github.com/Kurokio/HDRL-Internship-2024
```
2. In the directory directly above the HDRL-Internship-2024 directory, install the Scripts module.
```python
pip install ./HDRL-Internship-2024
```
If you intend to modify the scripts, execute this instead.
```python
pip install -e ./HDRL-Internship-2024
```

## Usage
Follow the notebook "HowToUse" which walks you through step-by-step how to do the actions listed above. 

For more information on adding additional fields to scrape and add to the database for analysis, refer to the "HowToAdd" notebook.

Follow the notebook "DataAccessTest" if you would like a tutorial for how to test for data accessibility.

## Contribution
Contributors and collaborators are welcome. Acceptable contributions can be documentation, code, suggesting ideas, and submitting issues and bugs.

Make sure to be nice when contributing and submitting commit messages, as this is a project developed by an intern that is still learning.

There will likely be minimal attention given to this project after 2024-08-12, but I will give it my best effort.

## Credits
Thanks to the following people who helped make this project a reality:
- <a href="https://github.com/rebeccaringuette" target="_blank">@rebeccaringuette</a>

## Contact
Contact me via LinkedIn or by using the email on my ORCiD page.
Zach Boquet - <a href="https://www.linkedin.com/in/zach-boquet-62a996254/" target="_blank">LinkedIn</a> - <a href="https://orcid.org/0009-0005-1686-262X" target="_blank">ORCiD</a>

## License
This project uses the following license: <a href="https://github.com/Kurokio/HDRL-Internship-2024/blob/main/LICENSE" target="_blank">Apache-2.0</a>
