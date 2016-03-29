package com.datastax.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;



import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

public class CassandraApplication {

    private static Cluster cluster;
    private static Session session;
    private static ResultSet results;
    private static Row row;

    public static void main(String[] args) {

        //connect to the cluster and keyspace cassandraDemo
        cluster = Cluster.builder()
                .addContactPoint("127.0.0.2")
                //since we are connected to a cluster this ensures that a default behavior to adopt when a node fails or request timeout
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy.Builder().withLocalDc("datacenter1").build()))
                .build();
        session = cluster.connect("cassandrademo");

        //prepared statements are only parsed once and the values are bound to variables and then we execute the bound statement to read and write to the cluster
        PreparedStatement statement = session.prepare(
                "INSERT INTO mycassandratable" + "(id, firstname, lastname, email, city, age)"
                                               + "VALUES (?,?,?,?,?,?);");
        BoundStatement boundStatement = new BoundStatement(statement);

        session.execute(boundStatement.bind("6468a7be-bfce-4c39-b819-b2c2244c4673,'Tyrone', 'Cutajar','tyrone.cutajar@email.com', 'London', '25'"));

        //Unlike part1 in this section I am using QueryBuilder not string representation of CQL to retrieve data, this is a more secure way as it discourages CQL injection attacks
        //use select to retrieve the user just created
        Statement select  = QueryBuilder.select().all()
                .from("cassandrademmo","mycassandratable")
                .where(eq("id","6468a7be-bfce-4c39-b819-b2c2244c4673"));

        results = session.execute(select);

        for ( Row row: results) {
            System.out.format("%s","%d\n", row.getString("firstname"), row.getInt("age"));
        }

        //Updating the age of the user data
        Statement update = QueryBuilder.update("cassandrademo","mycassandratable")
                                       .with(set("age","36"))
                                       .where(eq("id","6468a7be-bfce-4c39-b819-b2c2244c4673"));
                session.execute(update);

        //Delete the user from the table
        Statement delete = QueryBuilder.delete()
                                       .from("cassandrademo","mycassandratable")
                                       .where(eq("id","6468a7be-bfce-4c39-b819-b2c2244c4673"));
        session.execute(delete);

        //show that the user is gone
        select=QueryBuilder.select().all().from("cassandrademo","mycassandratable");
        results=session.execute(select);
        for(Row row : results) {
            System.out.format("%s","%id", row.getString("firstname"),row.getInt("age"));
        }
        //clean up the connection by clossing it
        cluster.close();
    }

}