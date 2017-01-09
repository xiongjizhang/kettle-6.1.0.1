package org.pentaho.di.trans.steps.insertupdate;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.StepInjectionMetaEntry;
import org.pentaho.di.trans.step.StepInjectionUtil;
import org.pentaho.di.trans.step.StepMetaInjectionEntryInterface;
import org.pentaho.di.trans.step.StepMetaInjectionInterface;
import org.pentaho.di.trans.steps.tableinput.TableInputMetaInjection.Entry;

public class InsertUpdateMetaInject implements StepMetaInjectionInterface{
	
	public enum Entry implements StepMetaInjectionEntryInterface {
	
		SCHEMANAME( ValueMetaInterface.TYPE_STRING, "what's the lookup schema?" ),
		TABLENAME( ValueMetaInterface.TYPE_STRING, "what's the lookup table?" ),
		DATABASEMETA( ValueMetaInterface.TYPE_SERIALIZABLE, "database connection" ),
		KEYSTREAM( ValueMetaInterface.TYPE_STRING, "which field in input stream to compare with?" );
	
	    private int valueType;
	    private String description;
	
	    private Entry( int valueType, String description ) {
	      this.valueType = valueType;
	      this.description = description;
	    }
	
	    /**
	     * @return the valueType
	     */
	    public int getValueType() {
	      return valueType;
	    }
	
	    /**
	     * @return the description
	     */
	    public String getDescription() {
	      return description;
	    }
	
	    public static Entry findEntry( String key ) {
	      return Entry.valueOf( key );
	    }
	}
	
	private InsertUpdateMeta meta;

	public InsertUpdateMetaInject( InsertUpdateMeta meta ) {
		this.meta = meta;
	}

	@Override
	public List<StepInjectionMetaEntry> getStepInjectionMetadataEntries()
			throws KettleException {
		// TODO Auto-generated method stub
		List<StepInjectionMetaEntry> all = new ArrayList<StepInjectionMetaEntry>();

	    Entry[] topEntries =
	      new Entry[] {
	        Entry.SCHEMANAME, Entry.TABLENAME, Entry.DATABASEMETA, Entry.KEYSTREAM, };
	    for ( Entry topEntry : topEntries ) {
	      all.add( new StepInjectionMetaEntry( topEntry.name(), topEntry.getValueType(), topEntry.getDescription() ) );
	    }

	    return all;
	}

	@Override
	public void injectStepMetadataEntries(List<StepInjectionMetaEntry> metadata)
			throws KettleException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<StepInjectionMetaEntry> extractStepMetadataEntries()
			throws KettleException {

	    return null;
	}

	public InsertUpdateMeta getMeta() {
		return meta;
	}

}
