# Assignment 02

Full name (Student ID Number)

You can answer in English or Bahasa Indonesia.

## Problem 1 (50 poin)
In this problem, you need to find all the years in which each actor or actress in the IMDb datasets performed.
To achieve this, you need two datasets from 
[IMDB Non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/)
in "Data Location" and download the following datasets:
1. `title.basics.tsv.gz`
2. `name.basics.tsv.gz`

Each file above is almost 1 GB in size after extraction, so ensure you have 
enough storage in your directory.
You can read these files directly with `.read_csv()` and set the appropriate 
`sep` and `header` options to match the dataset structure.

If your laptop is not fast enough to process these large datasets, try working 
with smaller portions of the data. To do this, read the data and write into 
sampled datasets. Alternatively, you can create a dummy dataset to test if your 
code works, and then apply it to the actual datasets.

Your program must produce the result in the following format

```bash
+---------+-------------------------+
| name    | year                    |
+---------+-------------------------+
| actor_0 | [year_00, year_01, ...] |
| actor_1 | [year_10, year_11, ...] |
| actor_2 | [year_20, year_21, ...] |
| ...     |                         |
| actor_N | [year_N0, year_N1, ...] |
+---------+-------------------------+
```

### [Bonus point: 20]    
Create a command prompt histogram where the number of the years of each actors is the number of `+` characters in the command
prompt histogram. 

Your histogram shoudl be like this

```bash
 actor_0 | +++++++++++++++++++
 actor_1 | +++++
 actor_2 | ++++++++++++
 actor_3 | +++++++++++++++++++++++++++++++
 ...
 actor_N | ++++
```
The name of `actor_#` should be right-aligned.
You need to sort the actor or actress names alphabetically.


## Problem 2 (20 points)

Store your result in Problem 1 into `.pdf`.
To achieve that, you need to convert your PySpark DataFrame into a Pandas 
DataFrame using `toPandas()` and then convert it into HTML using `.to_html()`. 
You can save it as an HTML file, open in a browser, and print or save it as 
a PDF file.  

### [Bonus point: 10]    
You can earn this bonus point if you automate the entire process from 
HTML string to `.pdf` without opening a browser and improve the table's 
appearance beyond the default. For example the rendered markdown version 
has a better presentation than the default HTML table. 
Each student must have a different table formats. 
All the procedures need to be done within your Jupyter notebook.