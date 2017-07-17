package ECB16S1;

import java.util.*;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;



public class List {
	
	//since we need to use this array list in Contact class in a static form, we put static in front
	static ArrayList<Contact> contactList = new ArrayList<Contact>();
	//build the array list for query
	private ArrayList<Integer> queryListName = new ArrayList<Integer>();
	private ArrayList<Integer> queryListBirth = new ArrayList<Integer>();
	private ArrayList<Integer> queryListPhone = new ArrayList<Integer>();

	
	//to read the phone book sample and add to an array list
	public void readContact(String phoneBook) {
		try{
			//split it by blank lines
			String[] temp = phoneBook.trim().split("\\r\\n[\\r\\n]+");
			for(int i=0; i<temp.length; i++){
				Scanner sc = new Scanner(temp[i]);
		       	String keyword, contain;
		       	Contact p = new Contact();
		       	while(sc.hasNext()){
		       		keyword = sc.next();
		       		if(sc.hasNextLine()){
			       	    contain = sc.nextLine().trim();
			       	    //if we find the keyword "name" add name
			       		if(keyword.equalsIgnoreCase("name")){       				
		       				p.addName(contain);  				
		       			}
			       	    //if we find the keyword "birthday" add birthday
			       		else if(keyword.equalsIgnoreCase("birthday")){       				
		       				p.addBirth(contain);  				
		       			}
			       	    //if we find the keyword "email" add email
			       		else if(keyword.equalsIgnoreCase("email")){       				
		       				p.addEmail(contain);  				
		       			}
			         	//if we find the keyword "phone" add phone
			       		else if(keyword.equalsIgnoreCase("phone")){       				
		       				p.addPhone(contain);  				
		       			}
			       	    //if we find the keyword "address" add address
			       		else if(keyword.equalsIgnoreCase("address")){ 
	    		       				p.addAddress(contain);  		         						
		       			}
			       		//if address is more than one line add it together
			       		else{
			       			p.addAddress(keyword + " " +contain);
			       		}
		       		}
		       		else{
		       			continue;
		       		}
		       	}
		       	//for all the data is valid to add, add to array list
		       	if(p.isValidToAdd()){
					contactList.add(p);
					}
			}
		}
	     catch (Exception ex) {
	         ex.printStackTrace();
	     }
		
	}

	
	//add name when run the add method
	public void addContact(String s){
		try{
			String[] split = s.trim().split(";");
	       	Contact p = new Contact();
			for(int i=0; i<split.length; i++){
				Scanner sc = new Scanner(split[i]);
		       	String keyword, contain;
		       	if(sc.hasNext()){
		       		keyword = sc.next();
		       		if(sc.hasNextLine()){
			       	    contain = sc.nextLine().trim();
			       	    //find the keyword "name" add name
			       		if(keyword.equalsIgnoreCase("name")){       				
		       				p.addName(contain);  				
		       			}
			       	    //find the keyword "birthday" add birthday
			       		if(keyword.equalsIgnoreCase("birthday")){       				
		       				p.addBirth(contain);  				
		       			}
			       	    //find the keyword "email" add
			       		if(keyword.equalsIgnoreCase("email")){       				
		       				p.addEmail(contain);  				
		       			}
			       	    //find the keyword "phone" add
			       		if(keyword.equalsIgnoreCase("phone")){       				
		       				p.addPhone(contain);  				
		       			}
			       	    //find the keyword "address" add
			       		if(keyword.equalsIgnoreCase("address")){       				
		       				p.addAddress(contain);  				
		       			}
		       		}else{
		       			continue;
		       		}
		       	}else continue;	

			}
			//if the data is not exist before and it is valid to add then add
	       	if(p.isValidToAdd()&&p.isNotExist()){
				contactList.add(p);
				}
	       	//if the data is exist and match the format then update the data
	       	if((!p.isNotExist())&&p.isValidToAdd()){
	       		p.toUpdate();
	       	}
		}
	     catch (Exception ex) {
	         ex.printStackTrace();
	     }
	}
	
	
	//run the delete method
	public void deleteContact(String s) throws ParseException{
		if(s.contains(";")){
			String[] temp = s.trim().split(";");
			if(temp[0]!=null && temp[1]!=null){
				Contact d = new Contact();
				d.addName(temp[0].trim());
				d.addBirth(temp[1].trim());
				if(d.isValidToDelete()){
					for(int i=0; i<contactList.size();i++){
						//if the same name and birthday are found then delete
						if(contactList.get(i).getName().equalsIgnoreCase(d.getName())&&contactList.get(i).getBirth().equalsIgnoreCase(d.getBirth())){
							contactList.remove(i);
						}
					}
				}
			}
		}


}

	
	//run the query method
	public void addQuery(String s){
		try{
			Scanner sc = new Scanner(s);
	       	String keyword, contain;
	       	while(sc.hasNext()){
	       		keyword = sc.next();
	       		if(sc.hasNextLine()){
	       			contain = sc.nextLine().trim();
	       			//if the keyword "name" is found then query for name add to query list name
	       			if(keyword.equalsIgnoreCase("name")){
	       				for(int i=0; i<contactList.size();i++){
	       					if(contactList.get(i).getName().equalsIgnoreCase(contain)){
	       						queryListName.add(i);
	       					}
	       				}
	       			}
	       		    //if the keyword "birthday" is found then query for birthday add to query list birthday
	       			if(keyword.equalsIgnoreCase("birthday")){
	       				for(int i=0; i<contactList.size();i++){
	       					if(contactList.get(i).getBirth().equals(contain)){
	       						queryListBirth.add(i);
	       					}
	       				}
	       			}
	       		    //if the keyword "phone" is found then query for phone add to query list phone
	       			if(keyword.equalsIgnoreCase("phone")){
	       				for(int i=0; i<contactList.size();i++){
	       					if(contactList.get(i).getPhone() != null){
		       					if(contactList.get(i).getPhone().equals(contain)){
		       						queryListPhone.add(i);	       					
		       					}
	       					}
	       				}
	       			}	       			
	       		}	       		
	       	}
		}
	     catch (Exception ex) {
	         ex.printStackTrace();
	     }
	}
	
	
	
	//to String the whole contact list
	public String toStringContact(){
		StringBuilder sb = new StringBuilder();
		for(Contact p: contactList){
			sb.append(p.toString());
			sb.append("\r\n\r\n");
		}
		return sb.toString();
	}
	
	
	
	//to String the query list
	public String toStringQuery(){
		//initial a string builder
		StringBuilder sb = new StringBuilder();
		
		//for the query list name is not empty
		if(!queryListName.isEmpty()){
			sb.append("====== query name " + contactList.get(queryListName.get(0)).getName() + " ======\r\n");			
			//for the date i is before date j, change the position of i and j 
			for(int i=0; i<queryListName.size();i++){
				for(int j=0; j<i; j++){
					SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy");
					try {
						Date birth1 = df.parse(contactList.get(queryListName.get(i)).getBirth());
						Date birth2 = df.parse(contactList.get(queryListName.get(j)).getBirth());
						if(birth1.before(birth2)){
							Collections.swap(queryListName, i, j);
						}
					} 
					catch (ParseException e) {
						e.printStackTrace();						
					}					
				}				
			}
			//print all the query name
			for(int p : queryListName){
				sb.append(contactList.get(p).toString());
				sb.append("\r\n\r\n");
				}
			sb.append("====== end of query name " + contactList.get(queryListName.get(0)).getName() + " ======\r\n\r\n");
		}
		
		//else print not found
		else{
			sb.append("Not found query Name\r\n\r\n");
		}
		
		
		//for the query list birthday is not empty
		if(!queryListBirth.isEmpty()){
			sb.append("====== query birthday " + contactList.get(queryListBirth.get(0)).getBirth() + " ======\r\n");
			//for the letter of name i is before name j, change the position of i and j 
			for(int i=0; i<queryListBirth.size();i++){
				for(int j=0; j<i; j++){
					if(contactList.get(queryListBirth.get(i)).getName().compareTo(contactList.get(queryListBirth.get(j)).getName())<0){
						Collections.swap(queryListBirth, i, j);
					}					
				}				
			}
			
			//print all the query list birthday
			for(int p : queryListBirth){
				sb.append(contactList.get(p).toString());
				sb.append("\r\n\r\n");
				}
			sb.append("====== end of query birthday " + contactList.get(queryListBirth.get(0)).getBirth() + " ======\r\n\r\n");
		}
		
		//else print not found
		else{
			sb.append("Not found query Birthday\r\n\r\n");
		}

		
		//for the query list phone is not empty
		if(!queryListPhone.isEmpty()){
			sb.append("====== query phone " + contactList.get(queryListPhone.get(0)).getPhone() + " ======\r\n");
			// sort the name in query list phone first
			for(int i=0; i<queryListPhone.size();i++){
				for(int j=0; j<i; j++){
					if(contactList.get(queryListPhone.get(i)).getName().compareTo(contactList.get(queryListPhone.get(j)).getName())<0){
						Collections.swap(queryListPhone, i, j);
					}					
				}				
			}
			
			//then if the name i is equal to the name j sort the i and j by date
			for(int i=0; i<queryListPhone.size();i++){
				for(int j=0; j<i; j++){
					if(contactList.get(queryListPhone.get(i)).getName().equalsIgnoreCase(contactList.get(queryListPhone.get(j)).getName())){
						SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy");
						try {
							Date birth1 = df.parse(contactList.get(queryListPhone.get(i)).getBirth());
							Date birth2 = df.parse(contactList.get(queryListPhone.get(j)).getBirth());
							if(birth1.before(birth2)){
								Collections.swap(queryListPhone, i, j);
							}
						} 
						catch (ParseException e) {
							e.printStackTrace();						
						}	
					}
				}
			}
			
			//print all the query list phone
			for(int p : queryListPhone){
				sb.append(contactList.get(p).toString());
				sb.append("\r\n\r\n");			
				}
			sb.append("====== end of query phone " + contactList.get(queryListPhone.get(0)).getPhone() + " ======\r\n\r\n");			
		}
		
		//else print not found
		else{
			sb.append("Not found query phone\r\n\r\n");
		}

		return sb.toString();
	}
	

}
