<?php
require_once 'Abstract.php';
require_once APPLICATION_ROOT . '/library/Phpcassa/connection.php';
require_once APPLICATION_ROOT . '/library/Phpcassa/columnfamily.php';
require_once APPLICATION_ROOT . '/library/Phpcassa/uuid.php';

class File_Cassandra extends File_Abstract 
{
    
    /**
     * Default Values
     */
    const DEFAULT_HOST = '127.0.0.1';
    const DEFAULT_PORT = '9160';
    
    /**
     * Available options
     *
     * =====> (array) servers :
     * an array of memcached server ; each memcached server is described by an associative array :
     * 'host' => (string) : the name of the memcached server
     * 'port' => (int) : the port of the memcached server
    */
    protected $_options = array (
        'servers' => array (
            array (
                'host' => self::DEFAULT_HOST,
                'port' => self::DEFAULT_PORT,
            )
        )
        ,'keyspace' => ''
        ,'columnfamily' => ''
    );
    
    /**
     * Phpcassa Connection object
     *
     * @var mixed phpcassa connection object
     */
    protected $_connection = null;
    
    /**
     * Phpcassa ColumnFamily object
     *
     * @var mixed phpcassa columnfamily object
     */
    protected $_columnFamily = null;
    
    
    /**
     * Constructor
     *
     * @param array $options associative array of options
     * @throws Exception
     * @return void
     */
    public function __construct(array $options = array())
    {
        parent::__construct($options);
        if (isset($this->_options['servers']))
        {
            $value= $this->_options['servers'];
            if (isset($value['host']))
            {
                // in this case, $value seems to be a simple associative array (one server only)
                $value = array(0 => $value); // let's transform it into a classical array of associative arrays
            }
            $this->setOption('servers', $value);
        }

        if (!array_key_exists('keyspace', $this->_options))
        {
            throw new Exception ('Undefined Keyspace for File_Cassandra object');
        }
        if (!array_key_exists('columnfamily', $this->_options))
        {
            throw new Exception ('Undefined ColumnFamily for File_Cassandra object');
        }
        foreach ($this->_options['servers'] as $key => $server)
        {
            if (!array_key_exists('port', $server))
            {
                $this->_options['servers'][$key]['port'] = self::DEFAULT_PORT;
            }
        }
        
        $this->_connection = new Connection (
            $this->_options['keyspace']
            ,$this->_options['servers']
        );
        try
        {
            $this->_columnFamily = new ColumnFamily (
                $this->_connection
                ,$this->_options['columnfamily']
            );
        } catch ( Exception $e )
        {
            throw new Exception ( "Cassandra ColumnFamily ['{$this->_options['columnfamily']}'] in Keyspace ['{$this->_options['keyspace']}'] does not exist" );
        }
    }
    
    /**
     * Destructor
     *
     * @return void
     */
    public function __destruct()
    {
        $this->_connection->close();
    }
    
    /* Store the file */
    public function write ( $id, $data, $metaData=array() )
    {
        if ( $this->exists($id) )
        {
            throw new Exception("File with this id ('{$id}') already exists");
        }
        $this->_columnFamily->insert (
            $id
            ,array (
                'data' => $data
                ,'attributes' => serialize($metaData)
                ,'timestamp' => time()
            )
        );
    }
    
    /* Replace the file, a measure to prevent unexpected overwriting */
    public function replace ( $id, $data, $metaData=array() )
    {
        $this->_columnFamily->insert (
            $id
            ,array (
                'data' => $data
                ,'attributes' => serialize($metaData)
                ,'timestamp' => time()
            )
        );
    }
    
    /* Retrieve the file data and attributes */
    public function read ( $id )
    {
        if ( !$this->exists($id) )
        {
            throw new Exception("File with id ('{$id}') does not exist");
        }
        /* 
         * In some cases, we might have to reformat the data to this format:
         * array (
         *    'data' => mixed
         *    ,'attributes => array
         *    ,'timstamp => int
         * )
         * But, in the case of Cassandra, we don't really need to.
         */
        $return = $this->_columnFamily->get($id);
        return array (
            'data' => $return['data']
            ,'attributes' => unserialize($return['attributes'])
            ,'timestamp' => $return['timestamp']
        );
        
    }
    
    /* Delete the file */
    public function delete($id)
    {
        $this->_columnFamily->remove($id);
    }
    
    /* Retrieve the file attributes */
    public function attributes($id)
    {
        if ( !$this->exists($id) )
        {
            throw new Exception("File with id ('{$id}') does not exist");
        }
        return array (
            'attributes' => $this->_columnFamily->get($id,"attributes")
            ,'timestampe' => $this->_columnFamily->get($id,"timestamp")
        );
    }
    
    /* Test whether the file exists, by counting how many columns are stored.
     * If 0/empty, then it doesn't exist
     */
    public function exists($id)
    {
        $result = $this->_columnFamily->get_count($id);
        return ( empty($result) ) ? false : true;
    }
    

}

