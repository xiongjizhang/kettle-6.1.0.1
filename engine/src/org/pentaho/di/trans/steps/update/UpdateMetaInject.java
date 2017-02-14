package org.pentaho.di.trans.steps.update;

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

public class UpdateMetaInject implements StepMetaInjectionInterface{
	
	public enum Entry implements StepMetaInjectionEntryInterface {
	
		DATABASE_META(ValueMetaInterface.TYPE_STRING, "database connection"),
		SCHEMA_NAME( ValueMetaInterface.TYPE_STRING, "what's the lookup schema?" ),
		TABLE_NAME( ValueMetaInterface.TYPE_STRING, "what's the lookup table?" ),
		COMMIT_SIZE( ValueMetaInterface.TYPE_STRING, "Commit size for inserts/updates" ),
		ERROR_IGNORED( ValueMetaInterface.TYPE_STRING, "update errors are ignored if this flag is set to true(Y/N)" ),
		IGNORE_FLAG_FIELD( ValueMetaInterface.TYPE_STRING, "adds a boolean field to the output indicating success of the update" ),
		SKIP_LOOKUP( ValueMetaInterface.TYPE_STRING, "adds a boolean field to skip lookup and directly update selected fields(Y/N)" ),
		USE_BATCH_UPDATE( ValueMetaInterface.TYPE_STRING, "Flag to indicate the use of batch updates, enabled by default but disabled for backward compatibility(Y/N)" ),
		
		KEY_FIELDS( ValueMetaInterface.TYPE_NONE, "The key fields" ),
	    KEY_FIELD( ValueMetaInterface.TYPE_NONE, "Key field" ),
	    KEY_LOOKUP( ValueMetaInterface.TYPE_STRING, "field in table" ),
	    KEY_CONDITION( ValueMetaInterface.TYPE_STRING, "Comparator: =, <>, BETWEEN, ..." ),
	    KEY_STREAM( ValueMetaInterface.TYPE_STRING, "which field in input stream to compare with?" ),
	    KEY_STREAM2( ValueMetaInterface.TYPE_STRING, "Extra field for between..." ),
	      
	    UPDATE_FIELDS( ValueMetaInterface.TYPE_NONE, "The update fields" ),
	    UPDATE_FIELD( ValueMetaInterface.TYPE_NONE, "Update field" ),
	    UPDATE_LOOKUP( ValueMetaInterface.TYPE_STRING, "Field value to update after lookup" ),
	    UPDATE_STREAM( ValueMetaInterface.TYPE_STRING, "Stream name to update value with" );
		  
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
	
	private UpdateMeta meta;

	public UpdateMetaInject( UpdateMeta meta ) {
		this.meta = meta;
	}

	@Override
	public List<StepInjectionMetaEntry> getStepInjectionMetadataEntries()
			throws KettleException {
		// TODO Auto-generated method stub
		List<StepInjectionMetaEntry> all = new ArrayList<StepInjectionMetaEntry>();
		
	    Entry[] topEntries =
	      new Entry[] {
	        Entry.DATABASE_META, Entry.SCHEMA_NAME, Entry.TABLE_NAME, Entry.COMMIT_SIZE, Entry.ERROR_IGNORED,
	        Entry.IGNORE_FLAG_FIELD, Entry.SKIP_LOOKUP, Entry.USE_BATCH_UPDATE};
	    for ( Entry topEntry : topEntries ) {
	      all.add( new StepInjectionMetaEntry( topEntry.name(), topEntry.getValueType(), topEntry.getDescription() ) );
	    }
	    
	    StepInjectionMetaEntry keyFieldsEntry =
	      new StepInjectionMetaEntry(
	        Entry.KEY_FIELDS.name(), ValueMetaInterface.TYPE_NONE, Entry.KEY_FIELDS.description );
	    all.add( keyFieldsEntry );
	    StepInjectionMetaEntry keyFieldEntry =
	      new StepInjectionMetaEntry(
	        Entry.KEY_FIELD.name(), ValueMetaInterface.TYPE_NONE, Entry.KEY_FIELD.description );
	    keyFieldsEntry.getDetails().add( keyFieldEntry );
	    Entry[] keyFieldsEntries = new Entry[] { Entry.KEY_LOOKUP, Entry.KEY_CONDITION, Entry.KEY_STREAM, Entry.KEY_STREAM2 };
	    for ( Entry entry : keyFieldsEntries ) {
	      StepInjectionMetaEntry metaEntry =
	        new StepInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
	      keyFieldEntry.getDetails().add( metaEntry );
	    }

	    StepInjectionMetaEntry valueFieldsEntry =
	      new StepInjectionMetaEntry(
	        Entry.UPDATE_FIELDS.name(), ValueMetaInterface.TYPE_NONE, Entry.UPDATE_FIELDS.description );
	    all.add( valueFieldsEntry );
	    StepInjectionMetaEntry valueFieldEntry =
	      new StepInjectionMetaEntry(
	        Entry.UPDATE_FIELD.name(), ValueMetaInterface.TYPE_NONE, Entry.UPDATE_FIELD.description );
	    valueFieldsEntry.getDetails().add( valueFieldEntry );   
	    Entry[] valueFieldsEntries = new Entry[] { Entry.UPDATE_LOOKUP, Entry.UPDATE_STREAM };
	    for ( Entry entry : valueFieldsEntries ) {
	      StepInjectionMetaEntry metaEntry =
	        new StepInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
	      valueFieldEntry.getDetails().add( metaEntry );
	    }

	    return all;
	}

	@Override
	public void injectStepMetadataEntries(List<StepInjectionMetaEntry> all)
			throws KettleException {
		// TODO Auto-generated method stub
		List<String> keyLookupList = new ArrayList<String>();
		List<String> keyConditionList = new ArrayList<String>();
		List<String> keyStreamList = new ArrayList<String>();
		List<String> keyStream2List = new ArrayList<String>();
	    List<String> updateLookupList = new ArrayList<String>();
	    List<String> updateStreamList = new ArrayList<String>();

	    // Parse the fields, inject into the meta class..
	    //
	    for ( StepInjectionMetaEntry lookFields : all ) {
	      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
	      if ( fieldsEntry == null ) {
	        continue;
	      }

	      String lookValue = (String) lookFields.getValue();
	      switch ( fieldsEntry ) {
	        case KEY_FIELDS:
	          for ( StepInjectionMetaEntry lookField : lookFields.getDetails() ) {
	            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
	            if ( fieldEntry == Entry.KEY_FIELD ) {
	            	List<StepInjectionMetaEntry> entries = lookField.getDetails();
	                for ( StepInjectionMetaEntry entry : entries ) {
	                  Entry metaEntry = Entry.findEntry( entry.getKey() );
	                  if ( metaEntry != null ) {
	                	  String value = (String) entry.getValue();
	                	  switch (metaEntry) {
		                	  case KEY_LOOKUP:
		                		  keyLookupList.add(value);
		                		  break;
		                	  case KEY_CONDITION:
		                		  keyConditionList.add(value);
		                		  break;
		                	  case KEY_STREAM:
		                		  keyStreamList.add(value);
		                		  break;
		                	  case KEY_STREAM2:
		                		  keyStream2List.add(value);
		                		  break;
	                		  default:
	                			  break;
	                	  }
	                  }
	                }
	            }
	          }
	          break;
	        case UPDATE_FIELDS:
	            for ( StepInjectionMetaEntry lookField : lookFields.getDetails() ) {
	              Entry fieldEntry = Entry.findEntry( lookField.getKey() );
	              if ( fieldEntry == Entry.UPDATE_FIELD ) {
	              	List<StepInjectionMetaEntry> entries = lookField.getDetails();
	                for ( StepInjectionMetaEntry entry : entries ) {
	                  Entry metaEntry = Entry.findEntry( entry.getKey() );
	                  if ( metaEntry != null ) {
	                    String value = (String) entry.getValue();
	                    switch (metaEntry) {
	                	  case UPDATE_LOOKUP:
	                		  updateLookupList.add(value);
	                		  break;
	                	  case UPDATE_STREAM:
	                		  updateStreamList.add(value);
	                		  break;
	              		  default:
	              			  break;
	                    }
	                  }
	                }
	              }
	            }
	            break;
	        case DATABASE_META:
	        	meta.setDatabaseMeta(DatabaseMeta.findDatabase(meta.getDatabaseMetas(), lookValue));
		        break;
	        case SCHEMA_NAME:
		        meta.setSchemaName( lookValue );
		        break;
	        case TABLE_NAME:
		        meta.setTableName( lookValue );
		        break;
	        case COMMIT_SIZE:
		        meta.setCommitSize( lookValue );
		        break;
	        case ERROR_IGNORED:
		        meta.setErrorIgnored( "Y".equalsIgnoreCase( lookValue ) );
		        break;
	        case IGNORE_FLAG_FIELD:
		        meta.setIgnoreFlagField( lookValue );
		        break;
	        case SKIP_LOOKUP:
		        meta.setSkipLookup( "Y".equalsIgnoreCase( lookValue ) );
		        break;
	        case USE_BATCH_UPDATE:
		        meta.setUseBatchUpdate( "Y".equalsIgnoreCase( lookValue ) );
		        break;
		    default:
		    	break;
	      }
	    }

	    // Pass the grid to the step metadata
	    //
	    if ( keyLookupList.size() > 0 ) {
	      meta.setKeyLookup( keyLookupList.toArray( new String[keyLookupList.size()] ) );
	      meta.setKeyCondition( keyConditionList.toArray( new String[keyConditionList.size()] ) );
	      meta.setKeyStream( keyStreamList.toArray( new String[keyStreamList.size()] ) );
	      meta.setKeyStream2( keyStream2List.toArray( new String[keyStream2List.size()] ) );
	    }
	    if ( updateLookupList.size() > 0 ) {
	      meta.setUpdateLookup( updateLookupList.toArray( new String[updateLookupList.size()] ) );
	      meta.setUpdateStream( updateStreamList.toArray( new String[updateStreamList.size()] ) );
	    }
	}

	@Override
	public List<StepInjectionMetaEntry> extractStepMetadataEntries()
			throws KettleException {

		List<StepInjectionMetaEntry> list = new ArrayList<StepInjectionMetaEntry>();
		
		list.add( StepInjectionUtil.getEntry( Entry.DATABASE_META, meta.getDatabaseMeta() ) );
	    list.add( StepInjectionUtil.getEntry( Entry.SCHEMA_NAME, meta.getSchemaName() ) );
	    list.add( StepInjectionUtil.getEntry( Entry.TABLE_NAME, meta.getTableName() ) );
	    list.add( StepInjectionUtil.getEntry( Entry.COMMIT_SIZE, meta.getCommitSize() ) );
	    list.add( StepInjectionUtil.getEntry( Entry.ERROR_IGNORED, meta.isErrorIgnored() ) );
	    list.add( StepInjectionUtil.getEntry( Entry.IGNORE_FLAG_FIELD, meta.getIgnoreFlagField() ) );
	    list.add( StepInjectionUtil.getEntry( Entry.SKIP_LOOKUP, meta.isSkipLookup() ) );
	    list.add( StepInjectionUtil.getEntry( Entry.USE_BATCH_UPDATE, meta.useBatchUpdate() ) );
	    
	    StepInjectionMetaEntry keyFieldsEntry = StepInjectionUtil.getEntry( Entry.KEY_FIELDS );
	    list.add( keyFieldsEntry );
	    for ( int i = 0; i < meta.getKeyLookup().length; i++ ) {
	    	StepInjectionMetaEntry fieldEntry = StepInjectionUtil.getEntry( Entry.KEY_FIELD );
	        List<StepInjectionMetaEntry> details = fieldEntry.getDetails();
	        details.add( StepInjectionUtil.getEntry( Entry.KEY_LOOKUP, meta.getKeyLookup()[i] ) );
	        details.add( StepInjectionUtil.getEntry( Entry.KEY_CONDITION, meta.getKeyCondition()[i] ) );
	        details.add( StepInjectionUtil.getEntry( Entry.KEY_STREAM, meta.getKeyStream()[i] ) );
	        details.add( StepInjectionUtil.getEntry( Entry.KEY_STREAM2, meta.getKeyStream2()[i] ) );
	        keyFieldsEntry.getDetails().add( fieldEntry );
	    }
	    
	    StepInjectionMetaEntry valueFieldsEntry = StepInjectionUtil.getEntry( Entry.UPDATE_FIELDS );
	    list.add( valueFieldsEntry );
	    for ( int i = 0; i < meta.getUpdateLookup().length; i++ ) {
	    	StepInjectionMetaEntry fieldEntry = StepInjectionUtil.getEntry( Entry.UPDATE_FIELD );
	        List<StepInjectionMetaEntry> details = fieldEntry.getDetails();
	        details.add( StepInjectionUtil.getEntry( Entry.UPDATE_LOOKUP, meta.getUpdateLookup()[i] ) );
	        details.add( StepInjectionUtil.getEntry( Entry.UPDATE_STREAM, meta.getUpdateStream()[i] ) );
	        valueFieldsEntry.getDetails().add( fieldEntry );
	    }

	    return list;
	}

	public UpdateMeta getMeta() {
		return meta;
	}

}
