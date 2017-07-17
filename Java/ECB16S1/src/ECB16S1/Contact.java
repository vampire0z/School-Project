package ECB16S1;

import java.util.*;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Contact{
	
	//instance fields
	private String name;
	private String birthday;
	private String address;
	private String phone;
	private String email;
	
	//constructor1
	public Contact(){
		name = null;
		birthday = null;
		address = null;
		phone = null;
		email = null;
	}
	
	//constructor2
	public Contact(String name, String birthday, String address, String phone, String email){
		this.name = name;
		this.birthday = birthday;
		this.address = address;
		this.phone = phone;
		this.email = email;
	}

	
	//when the scanner find keyword name, add name
	public String addName(String contain) {
		name = contain;
		return name;		
	}

	
	//when the scanner find keyword birthday, add birthday
	public String addBirth(String contain) {
		birthday = contain;
		return birthday;	
	}

	//add email
	public String addEmail(String contain) {
		email = contain;
		return email;	
	}

	//add phone
	public String addPhone(String contain) {
		phone = contain;
		return phone;	
	}

	//add address
	public String addAddress(String contain) {
		if(address!=null){
			address = address + contain;
		}
		else {
			address = contain;
		}
		return address;	
	}
	
	//check if the data is valid to add
	public boolean isValidToAdd() {
		return validName()&&validBirth()&&validEmail()&&validPhone()&&validAddress();
	}
	
	//check if the data is valid to delete
	public boolean isValidToDelete() {
		return validName()&&validBirth();
	}
	
	
	//check if the data is exist before
	public boolean isNotExist(){
		for(Contact p : List.contactList){
			// if both of the name and birthday are the same then it is exist
			if(p.getName().equalsIgnoreCase(name)&&p.getBirth().equalsIgnoreCase(birthday)){
				return false;
			}
		}
		return true;

	}
	
	//update the data
	public void toUpdate(){
		//for the data in contact list, if it is exist both name and birthday, then update
		//update name and birthday first, then update for those are matched the format and who are not empty
		//for those are empty, keep the original data
		for(int i=0; i<List.contactList.size();i++){
			if(List.contactList.get(i).getName().equalsIgnoreCase(name)&&List.contactList.get(i).getBirth().equalsIgnoreCase(birthday)){				
				List.contactList.set(i, new Contact(name, birthday, ((address != null) ? (address):(List.contactList.get(i).getAddress())), ((phone != null) ? (phone):(List.contactList.get(i).getPhone())), ((email != null) ? (email):(List.contactList.get(i).getEmail()))));
			}
		}
	}
	
	//name valid
	private boolean validName(){
		if(name != null && name.matches("[a-z A-Z]+")){
			return true;
		}else{
			return false;
		}
	}
	
	//phone valid
	private boolean validPhone(){
		// if the phone number is not empty and match the format return true
		if(phone != null && phone.matches("[0-9]+")){
			phone = phone.replaceFirst("^0+(?!$)", "");
			return true;
		}
		//or if the phone is empty return true
		else if(phone == null){
			return true;
		}
		else{
			phone = null;
			return true;
		}
	}
	
	//birthday valid
	private boolean validBirth(){
		//first to make sure the birthday is exist
	    if(birthday != null){	    	
			try {
				//import the simple date format to identify if the date is valid
				//import the calendar to identify if the date is before today
				Calendar c = Calendar.getInstance();
			    SimpleDateFormat df = new SimpleDateFormat("dd-MM-yyyy");		  
		    	df.setLenient(false);
	            Date birth = df.parse(birthday);
	            if(birth.before(c.getTime())){
	            	//change the format of birthday to the same format
	            	birthday = df.format(birth);
		            return true;
	            }
	            else return false;

		    } catch (ParseException pe) {
		      return false;
		    }
	    }
	    else return false;	   
	}
	
	//email valid
	private boolean validEmail(){
		if(email != null && email.matches("^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\])|(([a-zA-Z\\-0-9]+\\.)+[a-zA-Z]{2,}))$")){
			return true;
		}
		else if(email == null){
			return true;
		}
		else{
			email = null;
			return true;
		}
	}
	
	//address valid
	private boolean validAddress(){
		if(address != null){
			String[] temp = address.trim().split(",");
			for(int i=0; i<temp.length; i++){
				Scanner sc = new Scanner(temp[i]);
				String keyword, contain;
				while(sc.hasNext()){
					keyword = sc.next();
					if(sc.hasNextLine()){
						contain = sc.nextLine().trim();
						//to make sure that post address is valid
						//we could add any valid post address title here
						//even we could have a database for post
						//we just need to clear post address only with number
						if(keyword.equalsIgnoreCase("NSW")){
							if(contain.matches("[ 0-9]+")){
								return true;
							}
							else{
								address = null;
								return true;
							}
						}
					}
				}
			}
		}
		address = null;
		return true;
	}

	//method to get name birthday address email and to string all of them
	public String getName(){
		return name;
	}	
	public String getBirth(){
		return birthday;
	}	
	public String getAddress(){
		return address;
	}
	public String getPhone(){
		return phone;
	}	
	public String getEmail(){
		return email;
	}	
	
	//to String the data
	//for those are not empty, print them all
	//for those are empty, print nothing
	public String toString() {
	    return "Name: " + name +"\r\nBirthday: " + birthday + ((email != null) ? ("\r\nEmail: " + email):"")+((phone != null) ? ("\r\nPhone: " + phone):"") + ((address != null) ? ("\r\nAddress: " + address):"");
	}

}
