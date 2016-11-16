# Cloud-MapReduce

The Assignment consists of two questions :- 

1. MR Based inverted index selection  
2. MR Based twitter analysis. 

# MR Based inverted index selection.

This assignment makes use of the following files :- 

1. InvertedIndex.java

To compile the project, you can use maven.

``` mvn compile ```

This will download all the dependencies and compile the files and produce the respective `.class` files in directory `target/classes` 

After that, you need to create a jar file to run on hadoop cluster.

To create the jar file,

```jar -cvf $HOME/invertedindex.jar -C /path/to/target/classes/ .```

To run the jar file on a hadoop cluster, use

```hadoop jar $HOME/invertedindex.jar InvertedIndex input_directory output_directory```

Here, `input_directory` is the direcotry on HDFS , in which the files for which you want to create inverted index are present.
`output_directory`

If the command runs succesfully, this will create a file `part-r-00000` in the `output_directory`, you can view the contents of
this file using ,

``` hadoop dfs -cat output_directory/part-r-00000```


# MR Based Twitter Anaysis

To compile the project, use maven

``` mvn compile ```

After that to create a jar file, which will run on hadoop cluster,

``` jar cvf $HOME/twitter.jar -C /path/to/target/classes .```

To run twitter analysis, you need a file containing tweets in the format `tweet_number: tweet text` on each line. The file should be placed inside the `tweets_input` directory in HDFS.

``` hadoop jar $HOME/twitter.jar TwitterIndex tweets_input tweets_output```

This will produce a file `part-r-00000` inside `tweets_output` directory. Copy this file to a directory `CosineInput`in HDFS.

``` hadoop dfs -cp tweets_output/part-r-00000 CosineInput/```

After that run the jar again on cluster, using:

``` hadoop jar $HOME/twitter.jar CosineSimilarity CosineInput/ CosineOutput/```

This will create a file `part-r-00000` inside the directory `CosineOutput` which contains the pair of tweets with ore than 90% similarity, which is the desired result.

