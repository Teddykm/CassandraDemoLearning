package com.datastax.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;


import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class CassandraApplication {

    private static Cluster cluster;
    private static Session session;
    private static ResultSet results;
    private static Row row;

    public static void main(String[] args) {

        //connect to the cluster and keyspace cassandraDemo
       Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                //since we are connected to a cluster this ensures that a default behavior to adopt when a node fails or request timeout
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy.Builder().withLocalDc("datacenter1").build()))
                .build();
        Session session = cluster.connect("cassandrademo");

        //prepared statements are only parsed once and the values are bound to variables and then we execute the bound statement to read and write to the cluster
        PreparedStatement statement = session.prepare(
                "INSERT INTO mycassandratable" + "(id, firstname, lastname, email, city, age)"
                                               + "VALUES (?,?,?,?,?,?);");
        BoundStatement boundStatement = new BoundStatement(statement);

        session.execute(boundStatement.bind(UUID.fromString("6468a7be-bfce-4c39-b819-b2c2244c4673"),"Tyrone", "Cutajar","tyrone.cutajar@email.com", "London",  Integer.parseInt("25")));

        //Unlike part1 in this section I am using QueryBuilder not string representation of CQL to retrieve data, this is a more secure way as it discourages CQL injection attacks
        //use select to retrieve the user just created
        Statement select  = QueryBuilder.select().all()
                .from("cassandrademo","mycassandratable")
                .where(eq("id",UUID.fromString("6468a7be-bfce-4c39-b819-b2c2244c4673")))
                .and(eq("lastname","Cutajar"));

        results = session.execute(select);

        for ( Row row: results) {
            System.out.format("%s, %s", row.getString("firstname"), row.getString("lastname"));
        }

        //Updating the age of the user data
        Statement update = QueryBuilder.update("cassandrademo","mycassandratable")
                                       .with(set("age",Integer.parseInt("36")))
                                       .where(eq("id",UUID.fromString("6468a7be-bfce-4c39-b819-b2c2244c4673")))
                                       .and(eq("lastname","Cutajar"));
                session.execute(update);

        //Delete the user from the table
        Statement delete = QueryBuilder.delete()
                                       .from("cassandrademo","mycassandratable")
                                       .where(eq("id",UUID.fromString("6468a7be-bfce-4c39-b819-b2c2244c4673")))
                                       .and(eq("lastname","Cutajar"));
        session.execute(delete);

        //show that the user is gone
        select=QueryBuilder.select().all().from("cassandrademo","mycassandratable");
        results=session.execute(select);
        for(Row row : results) {
            System.out.format("%s","%id", row.getString("firstname"),row.getInt("age"));
        }
        // clean up the connection by clossing it
        cluster.close();
    }

}