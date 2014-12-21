package DTree;

import java.util.ArrayList;
import java.util.List;

public class AttributeData{
	@SuppressWarnings("rawtypes")
	public List attrIndex;
	@SuppressWarnings("rawtypes")
	public List attrVal;
	double entophy;
	String classLabel;
	AttributeData()
	{
		 this.attrIndex= new ArrayList<Integer>();
		 this.attrVal = new ArrayList<String>();
		
		
	}
	AttributeData(List attr_index,List attr_value)
	{
		this.attrIndex=attr_index;
		this.attrVal=attr_value;
		
	}
	
	void add(AttributeData obj)
	{
		this.add(obj);
	}
	

}
