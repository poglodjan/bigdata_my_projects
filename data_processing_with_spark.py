import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, to_date, month, count, max, sum, length, regexp_replace

def main():
    spark = SparkSession.builder.appName("Tennis_WTA").getOrCreate()
    #read all files using any spark interface
    match = spark.read.option("header","true").option("inferschema","true").csv("hdfs://localhost:8020/user/vagrant/test2/spark/matches.csv")
    player = spark.read.option("header","true").option("inferschema","true").csv("hdfs://localhost:8020/user/vagrant/test2/spark/players.csv")
    tour = spark.read.option("header","true").option("inferschema","true").csv("hdfs://localhost:8020/user/vagrant/test2/spark/tournaments.csv")
    
    #a)
    #data frame that show the players who played most matches in the time period covered by the data,
    #player is winner_id or loser_id
    player_matches = ( 
        match.select(col("winner_id").alias("player_id")). 
        union(match.select(col("loser_id").alias("player_id"))).
        groupBy("player_id").agg(count("*").alias("match_count"))
    )
    #including all player information and sorting decreasingly by the number of matches, display the content with show()
    merged_matches = player_matches.join(player, 'player_id').sort('match_count', ascending=False)
    merged_matches.show()
    #show the number of records, save as parquet in the hdfs
    print(f"There are {merged_matches.count()} records")
    merged_matches.write.parquet("/user/vagrant/test2/spark/parquet")
    
    #b)
    #Create a data frame that will show the players that played most sets in the time period covered by the data
    #including all player information, and sorted decreasingly by the number of sets, 
    #display the content with show(), also show the number of records in the dataframe
    player_set = (
        match.select(col("winner_id").alias("player_id"), col("score"))
        .union(
            match.select(col("loser_id").alias("player_id"), col("score"))
        )
    )
    # column with number of sets in each match:
    player_set = player_set.withColumn("sets_played",
                                       length(regexp_replace(col("score"), "[^\\-]", ""))
                                       )
    # group by each player and sum of the sets
    player_set = player_set.groupBy("player_id").agg(count("*").alias("sets_count"))
    max_set = player_set.join(player, "player_id").sort("sets_count", ascending=False)

    max_set.show()
    max_set.count()
    
    #c)
    #Calculate the proportion of matches won by a left-handed player when playing a righthander
    # adding column with winning hand information
    players_with_hands = match.join(
        player.withColumnRenamed("player_id", "winner_id").withColumnRenamed("hand", "winner_hand"), "winner_id"
    ).join(
        player.withColumnRenamed("player_id", "loser_id").withColumnRenamed("hand", "loser_hand"), "loser_id"
    )

    # number of matches where left hand won
    left_hand_wins = players_with_hands.filter(
        (col("winner_hand") == "L") & (col("loser_hand") == "R")
    ).count()

    # number of all matches left hand vs right hand
    left_vs_right = players_with_hands.filter(
        ((col("winner_hand") == "L") & (col("loser_hand") == "R")) |
        ((col("winner_hand") == "R") & (col("loser_hand") == "L"))
    ).count()

    proportion = left_hand_wins / left_vs_right
    print(f"Proportion of matches won by left-handed players: {proportion:.2f}")

    return None

if __name__ == '__main__':
    main()
    print("Program completed successfully")
    sys.exit()

