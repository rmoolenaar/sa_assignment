# Assignment

Solution for the given assignment as part of a new project for a client of mine

## Code explanation

### Main idea
The main objective for me is to load the data in a way production versions can utilise this approach.
That's why the CSV data is first loaded directly into a standard Spark file, but then partitioned and
saved into a Spark CSV data file.
For this assignment the data is saved into 100 partitions. This will improve performance in a real
production environment. And, for this assignment, makes it easier to create batched/streamed data
for the window/slide assignment as files can be read into the stream in batches (currently 10 files per batch)

### Calculating the output
For calculating the output standard Spark dataframe operations are used. However, there is one observation
to be made: ranking the data based on the count (for the window/slide assignment) is not possible for
streamed data. Only timestamp windows are allowed without the 'over' operation.
That's the reason the data is outputted to memory. After the query terminates, the saved data in memory
can be queried/windowed/partitioned using all standard Spark dataframe operations.

### Drawbacks
I do not like the way the queries are terminated using the timeout parameter. This is not particular
useful in a production environment, as streams are, by default, considered to be run for a very long time.
## Running Test
The code can be tested using scalatest, where $CLASSPATH contains the scalatest Jar:
    % sbt package
    % scala -cp $CLASSPATH org.scalatest.run nl.javadb.LoadDataSpec

Or easier: via your preferred IDE (like IntelliJ which I used)

## Docker file
The docker file will load the bitnami/spark image which has a pre-installed Spark version inside a Unix environment.
That's the one of the main reasons to use Docker, as the pre-installed Spark version saves a lot of
effort and building time.
Next to this, there are several procedures that can be found on the Internet to load a Spark cluster via Docker images
to build your own Spark cluster. This makes this option for me well suited.
I assume Kubernetes can do this as well, but for the above mentioned reasons made me choose Docker.
To build the image and then run it (Docker needs to be running locally):
    % ./builddocker.sh
    % ./rundocker.sh

Have a nice day
