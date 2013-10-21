<?php




class File_Abstract
{
    /**
     * Available options
     *
     * @var array available options
     */
    protected $_options = array();
    
    /* Methods */
    public function __construct(array $options = array())
    {
        while (list($name, $value) = each($options)) {
            $this->setOption($name, $value);
        }
    }
    
    /**
     * Set an option
     *
     * @param  string $name
     * @param  mixed  $value
     * @throws Exception
     * @return void
     */
    public function setOption($name, $value)
    {
        if (!is_string($name)) {
            throw new Exception("Incorrect option name : $name");
        }
        $name = strtolower($name);
        if (array_key_exists($name, $this->_options)) {
            $this->_options[$name] = $value;
        }
    }
    
    /* Store the file */
    public function write($name) { }
    
    /* Retrieve the file */
    public function replace($name) { }
    
    /* Retrieve the file */
    public function read($name) { }
    
    /* Delete the file */
    public function delete($name) { }
    
    /* Retrieve the file attributes */
    public function attributes($name) { }
    
    /* Test whether the file exists */
    public function exists($name) { }
    
}

