
import java.util.*;
import java.io.*;
 
import javax.xml.parsers.*;
 
import org.w3c.dom.*;
 
import org.xml.sax.SAXException;
 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
 
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
public class DOMListProcessor {
 
	private List<Element> buffer = new ArrayList<Element>();
	
	private boolean matchingPair(Element n1, Element n2) {
		if(n1.getElementsByTagName("ner").item(0).getTextContent().equals(n2.getElementsByTagName("ner").item(0).getTextContent())) {
			return true;
		}
		return false;
	}
	
	private void add(Element node) {
		// if 0 ignore
		String NERtag = node.getElementsByTagName("ner").item(0).getTextContent();
		if(buffer.isEmpty() && NERtag.equals("O")) {
			buffer.add(node);
            processBuffer();
		} else if(NERtag.equals("O")) {
			// add to the buffer if empty
			processBuffer();
            buffer.add(node);
            processBuffer();
        } else if(buffer.isEmpty()) {
            buffer.add(node);
		} else {
			// if the buffer is not empty, compare Element nodes and add if true
			if(matchingPair(node, buffer.get(buffer.size() - 1))) {
				buffer.add(node);
			} else {
				processBuffer();
			}
		}
	}
	
	private void processBuffer() {
		if(buffer.size() != 1) {
			StringBuilder compoundNER = new StringBuilder();
            for(Element node : buffer) {
				// A conditional statement to print just the Organisation. 
				// The other Element nodes have not been removed from the buffer, in case the same logic needs to apply to them

                //if(!node.getElementsByTagName("ner").item(0).getTextContent().equals("O")) {
                    //System.out.print(node.getElementsByTagName("text").item(0).getTextContent());
                    // Process the <word> tag here.
                    compoundNER.append(String.format("%s",node.getElementsByTagName("text").item(0).getTextContent()));
                //}
			}
            exportBuffer(compoundNER.toString());
		} else {
            exportBuffer(null);
		}
		buffer.clear();
	}

    private void exportBuffer(String compoundNER){

        // Create a StringBuilder to hold the output
        StringBuilder outputString = new StringBuilder();

        // Iterate through all nodes in the buffer
        for(Element node : buffer) {
            outputString.append(String.format("%s|%s|%s|%s\n",
                    node.getElementsByTagName("id").item(0).getTextContent(),
                    node.getElementsByTagName("text").item(0).getTextContent(),
                    node.getElementsByTagName("ner").item(0).getTextContent(),
                    compoundNER));
        }
        System.out.print(outputString.toString());
    }
	
	public void run() throws Exception {
		// Get and Parse XML document into DOM
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		Document doc = db.parse(new File("input/basic.xml"));
		
		NodeList sentences = doc.getElementsByTagName("root");
		for(int i = 0; i < sentences.getLength(); i++) {
			// Check the node type for <sentence> tag
			Node sentence = sentences.item(i);
			Element sentenceElement = (Element) sentence;
			
			NodeList tokens = sentenceElement.getElementsByTagName("token");
			for(int x = 0; x < tokens.getLength(); x++) {
				Node token = tokens.item(x);
				Element tokenElement = (Element) token;
				
				add(tokenElement); 
 
			}
		}
	
	}
	
	public static void main(String[] args) throws Exception {
		DOMListProcessor listProcessor = new DOMListProcessor();
		listProcessor.run();
	}
	
	
}