package package1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.impl.recommender.ByValueRecommendedItemComparator;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class AggregateAndRecommendReducer extends Reducer<VarLongWritable,VectorWritable,VarLongWritable,RecommendedItemsWritable>
{
//...
	int recommendationsPerUser=1;
	public void reduce(VarLongWritable key,Iterable<VectorWritable> values,Context context) throws IOException, InterruptedException 
	{
		Vector recommendationVector = null;
		for (VectorWritable vectorWritable : values) 
		{
			recommendationVector = recommendationVector == null ?vectorWritable.get() :recommendationVector.plus(vectorWritable.get());
		}
		Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(recommendationsPerUser + 1,Collections.reverseOrder(ByValueRecommendedItemComparator.getInstance()));
		Iterator<Vector.Element> recommendationVectorIterator =recommendationVector.iterateNonZero();
		while (recommendationVectorIterator.hasNext()) 
		{
			Vector.Element element = recommendationVectorIterator.next();
			int index = element.index();
			float value = (float) element.get();
			if (topItems.size() <3)//recommendationsPerUser
			{
				topItems.add(new GenericRecommendedItem(index, value));
			} 
			else if (value >=topItems.peek().getValue()) 
			{
				topItems.add(new GenericRecommendedItem(index, value));
				topItems.poll();
			}
		}
		List<RecommendedItem> recommendations =new ArrayList<RecommendedItem>(topItems.size());
		recommendations.addAll(topItems);
		Collections.sort(recommendations,
				ByValueRecommendedItemComparator.getInstance());
		context.write(key,new RecommendedItemsWritable(recommendations));
}
}
