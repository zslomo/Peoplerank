package peoplerank;

import java.io.IOException;


public class PeopleRankDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        AdjacencyMatrix.run();
        for (int i = 0; i < 50; i++) {
           CalcPeopleRank.run();
        }
        FinallyResult.run();
    }
}
