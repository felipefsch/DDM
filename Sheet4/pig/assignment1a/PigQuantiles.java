package assignment1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class PigQuantiles extends EvalFunc<Double>{

	@Override
	public Double exec(Tuple tuple) throws IOException {
		Tuple p = (Tuple) tuple.get(0);
		
		//System.out.println("INPÜÜÜÜÜÜÜÜÜT " + p);
		
		int pos1 = (Integer) p.get(0);
		int pos2 = (Integer) p.get(1);
		DataBag bag = (DataBag) p.get(2);
		
		Tuple t = null;
		Tuple t1 = null;
		Tuple t2 = null;
		
		//System.out.println("########" + pos1 + " " + pos2 + " " + bag);
		
		Iterator it = bag.iterator();
		
		int i = 1;
		
		while (it.hasNext()) {
			t = (Tuple) it.next();
			//System.out.println("#########" + t.get(0));
			
			if (i == pos1)
				t1 = t;
			
			if (i == pos2)
				t2 = t;
			
			i++;
		}
		
		double quartile = ((double) t1.get(0) + (double) t2.get(0)) / 2;
		
		return quartile;
	}
}
