# Installation

The following tutorial is the installation of PySpark and its
configureation. We divide the installation
steps into two parts. First is the installation of Python through 
Miniconda, and we use it via Jupyter Notebook inside VSCode. 
This tutorial is tested under Windows 11 23H2.
After that we install PySpark and continue with PySpark's configuration.

We use VSCode because it is more lightweight than using
PyCharm or Spyder and it supports Jupyter Notebook with a full of ease
using VSCode extension.

## Python + VSCode + Jupyter Noteboook installation

### VSCode
We need VSCode to run the interface of Jupyter Notebook and to do command
editing. To install it, we can proceed with the following steps.

1. First, we need to download VSCode installer from the the following link:
   [https://code.visualstudio.com/Download](https://code.visualstudio.com/Download). Choose the installer for Window 11.

   <img src="./img-resources/vscode-installer.png" width=400>

2. Follow the instructions that are given during installation processes.
   You can click "Next" or "Yes". After the installation has been done,
   the "Welcome Page" will be shown. If it is not shown like in the figure, 
   open VSCode application manually from Start Menu.

   <img src="./img-resources/vscode-welcome-page.png" width=400>

At this moment, VSCode has been installed to your computer, and we ready to
continue to the next steps which is Miniconda installation.

### Miniconda
Miniconda is a smaller version than its parent program (Anaconda). This program
is used to manage Python version that is installed to your computer. 
So we can have many version of Python programs and different version of
installed libraries. This can be achieved by using what is so called 
"environment". Most of developers use single environment for a single project.

When we have environment, we can installed without any worries about 
dependency conflicts between Python and its libraries. In this course, 
we only use "base" environment which is a default environment. There are
steps to set a new environment with different Python's version and
libraries. You can explore more about this environment management in 
[Miniconda/Anaconda documentation](https://docs.conda.io/projects/conda/en/stable/user-guide/getting-started.html)

In here, we only install for Windows users. Please proceed to the following 
steps
1. Visit the download page of Miniconda in
   [https://docs.conda.io/projects/miniconda/en/latest/](https://docs.conda.io/projects/miniconda/en/latest/).
   Then select `Miniconda3 Windows 64-bit`.

   <img src="./img-resources/miniconda-download-page.png" width=400>

2. (optional) If you want to check that the downloaded file is really the 
   intended Miniconda installer, we can verify using SHA256 by typing the
   following command in Windows PowerShell. First you need to direct
   the directory of PowerShell into the location of the downloaded file
   of Miniconda installer. You can use command `cd`. Now type this
   
   ```
   Get-FileHash .\Miniconda3-latest-Windows-x86_64.exe -Algorithm SHA256
   ```

   Then, check if the code of SHA256 that is produced with the above command
   is the same as the code in the download page. If it is not the case,
   your download file is not authentic at all. It has beed tampered by
   someone and it is not safe to install it.

3. Double click to the Miniconda installer file 
   (`iniconda3-latest-Windows-x86_64.exe`). During the installation, 
   use default setting.

4. After installation finished, you can find a program with the name
   "Anaconda Prompt (Miniconda 3)" from the Start Menu. Open that program
   to initiate the base environment.

   <img src="./img-resources/miniconda-prompt.png" width=400>

At this moment, you have installed VSCode and Python and ready to continue
for the VSCode extensions and Python modules installation.

### Jupyter Notebook and other extensions and Python modules
To be able opening, editing, and running Jupyter Notebook files, we need
to install these three extension

1. Jupyter (by Microsoft)
2. Python (by Microsoft)
3. Markdown Preview Github Styling (by Matt Bierner)

Make sure that the above extensions that you are going to install have
the same name for its author (the name inside the parenthesis). There
are many extensions that share the same name but different author's name.

The following steps are only for Jupyter Notebook installation. For
the other extensions follows the same procedures.

1. Open VSCode, left click on the extension icon (an icon that is represented
   with four small blocks where the single block in the upper right is shifted).

   <img src="./img-resources/vscode-extension.png" width=400>

2. In the "Searc box", type "Jupyter". Then left click to the blue button
   with "Install". If you have done the installation for this extension,
   that blue button will not appear. You can continue the same steps
   for the two others extensions.

3. Reopen "Anaconda Prompt (Miniconda 3)" from the Start Menu, and 
   type the following commands to create new environment with the 
   name "learnPySpark". We also install some Python modules for the 
   purpose of this course

   ```
   conda deactivate
   conda create --name learnPySpark python=3.11
   conda activate learnPySpark
   ```

   Close the program after you have activated `learnPySpark`.

## PySpark installation
This is the long process of installation of PySpark in my opinion.
Follow the steps carefully, and please ask in the class if you have a problem.

### Java
There are two options of Java that we can use for PySpark installation. 
These are Java 8 and 11. But we stick to Java 8 because of compatibility 
issues in Java 11. Java 11 has many conflicts with certain third party 
libraries

1. Visit the following page to download Java 8 LTS (in the form of JDK - Java
   Development Kit) in Windows for x64 (64-bit) architecture 
   [Eclipse Temurin (TM) Latest Release](https://adoptium.net/temurin/releases/?os=windows&arch=x64&package=jdk&version=8). 
   Choose the installter with `.msi` file extension.

2. (optional) Do the checksum SHA254 to the downloaded file. See 
   the same procedure when you verify Miniconda.

3. Follow Windows's installation steps in 
   [https://adoptium.net/installation/windows/](https://adoptium.net/installation/windows/)

### Apache Spark
1. Download the latest version of Apache Spark from 
   [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html). In our case, we use spark `3.5.2 (Aug 10 2024)`
   with a package type `Pre-build for Apache Hadoop 3.3 and later`.

2. (optional) Do the checksum SHA254 to the downloaded file. See 
   the same procedure when you verify Miniconda.

3. Extract Apache Spark installation file (`spark-3.5.2-bin-hadoop3.tgz`)
   into `C:\Users\[your user name]`.

4. Rename `spark-x.x.x-bin-haddop#` into `spark`. The sequences
   `x.x.x` and symbol `#` represent the version of PySpark and Hadoop.
   You need to write down these numbers because we use them later.

5. Download `winutils` for the Hadoop version `3.3.6` from 
   [https://github.com/cdarlint/winutils/tree/master](https://github.com/cdarlint/winutils/tree/master).
   To download a single directory instead for all versions, you can use
   the following website [https://download-directory.github.io/](https://download-directory.github.io/) and paste the directory of `hadoop-3.3.6`.

6. Extract `hadoop-3.3.6` and put all the files inside it into 
   `C:\Users\[your user name]\hadoop\`.

## Configure PySpark

### Python modules installation

```
conda activate learnPySpark
pip install pandas ipython ipykernel pyspark
```

### Environment variables settings

```conf
spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true"
spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true"
```

Set the environment variables of Hadoop and PySpark 

