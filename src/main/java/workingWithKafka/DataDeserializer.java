package workingWithKafka;

import java.text.ParseException;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DataDeserializer {

    public ArrayList<UserVisitEntity> deserializeData (ArrayList<String> kafkaMessages) throws ParseException {
        ArrayList<UserVisitEntity> userVisits = new ArrayList<UserVisitEntity>();

        for(int i=0; i< kafkaMessages.size(); i++){
            String [] userVisitValues = kafkaMessages.get(i).split(",");
            UserVisitEntity userVisit = new UserVisitEntity(
                    userVisitValues[0].split(":")[1], userVisitValues[1].split(":")[1], userVisitValues[2].split(":")[1],Float.parseFloat(userVisitValues[3].split(":")[1]),userVisitValues[4].split(":")[1],userVisitValues[5].split(":")[1],
                    userVisitValues[6].split(":")[1],userVisitValues[7].split(":")[1],Integer.parseInt(userVisitValues[8].split(":")[1]));
            userVisits.add(userVisit);
        }
        return userVisits;
    }
}
