package ECB16S1;
import java.util.*;
import java.io.*;
public class Read {
	
	private File phoneBookFile;
	private File instructionFile;
	private File outputFile;
	private File reportFile;
	List p = new List();

	
	//read the name of each file
	public Read(String[] s){
	phoneBookFile = new File(s[0]);
	instructionFile = new File(s[1]);
	outputFile = new File(s[2]);
	reportFile = new File(s[3]);
	}
	
	
	// read the name of each file
	//this is just for the test
//	public Read(){
//		Scanner input = new Scanner(System.in);
//		System.out.print("phoneSample ");
//		phoneBookFile = new File(input.nextLine());
//		System.out.print("instruction ");
//		instructionFile = new File(input.nextLine());
//		System.out.print("output ");
//		outputFile = new File(input.nextLine());
//		System.out.print("report ");
//		reportFile = new File(input.nextLine());
//	}

	
	//Read the phone sample file
	public void readPhoneSample() {
		try {
            Scanner input = new Scanner(phoneBookFile);            
            while (input.hasNextLine()) {
            	// input all the text
                String phoneBook = input.useDelimiter("\\A").next();
                p.readContact(phoneBook);
            }                                        
            input.close(); 
		}
		catch (Exception ex) {
            ex.printStackTrace();
        }
	}

	
	//read the instruction sample file
	public void readInstructionSample(){
		try {
			Scanner input = new Scanner(instructionFile);
			while (input.hasNextLine()) {
				String instruction = input.useDelimiter("\\A").nextLine();
				//split the text by the blank line
				String[] temp = instruction.trim().split("\\r\\n[\\r\\n]+");
				for(int i=0; i<temp.length; i++){
					Scanner sc = new Scanner(temp[i]);
					String keyword, contain;
					while(sc.hasNext()){
						keyword = sc.next();
						// if the keyword "save" is found save the text
	        			if(keyword.equalsIgnoreCase("save")){
	        				try{
	        					PrintStream out = new PrintStream(new FileOutputStream(outputFile));
	        					System.setOut(out);
	        					out.println(p.toStringContact());
	        					out.close();
	        					PrintStream out1 = new PrintStream(new FileOutputStream(reportFile));
	        					System.setOut(out1);
	        					out1.println(p.toStringQuery());
	        					out1.close();
	        				} 
	        				catch(FileNotFoundException e){
	        					e.printStackTrace();
	        				}
	        			}
						if(sc.hasNextLine()){
							contain = sc.nextLine().trim();
							//if the keyword "add" is found
	            			if(keyword.equalsIgnoreCase("add")){
	            				p.addContact(contain);
	            			}
	            			//if the keyword "delete" is found
	            			if(keyword.equalsIgnoreCase("delete")){
	            				p.deleteContact(contain);
	            			}
	            			//if the keyword "query" is found
	            			if(keyword.equalsIgnoreCase("query")){
	            				p.addQuery(contain);
	            			}
						}						
					}					
				}				
			}
			input.close(); 
		}
		catch (Exception ex) {
            ex.printStackTrace();
        }
	}

}
