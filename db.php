<?php

/* 
 * The MIT License
 *
 * Copyright 2015 Li Feilong.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/**
 * 注意mysql保留字
 * PDO mysql wrapper
 * @author Li Feilong <feiyang8068@qq.com>
 */
class DbMysql {
    /**
     * pdo connection instance
     * @var PDO
     */
    protected $_db;
    /**
     * example:
     * mysql:host=localhost;dbname=testdb
     * mysql:host=localhost;port=3306;dbname=testdb
     * mysql:unix_socket=/tmp/mysql.sock;dbname=testdb
     * @var string
     */
    protected $_dsn;
    
    protected $_username;
    
    protected $_password;
    
    protected $_encoding;

    /**
     * log record
     * @var array
     */
    protected $_debug_log = array();

    /**
     * config param
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param string $encoding
     */
    public function __construct($dsn, $username, $password, $encoding='utf8') {
        $this->_dsn = $dsn;
        $this->_username = $username;
        $this->_password = $password;
        $this->_encoding = strtolower($encoding);
        $this->connect();
    }
    
    /**
     * SET NAMES utf8; 
     * SET character_set_client = utf8;
     * SET character_set_results = utf8;
     * SET character_set_connection = utf8; 
     * @throws Exception
     */
    public function connect() {
        if ($this->_db === NULL) {
            try {
                $set_names_sql = "SET NAMES '{$this->_encoding}'";
                $options = array(PDO::MYSQL_ATTR_INIT_COMMAND => $set_names_sql);
                $this->_db = new PDO($this->_dsn, $this->_username, $this->_password, $options);
                // 设置抛异常
                $this->_db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION); 
                if (version_compare(PHP_VERSION, '5.3.6')) {
                    $this->_db->exec($set_names_sql);
                }
            } catch (Exception $e) {
                echo $e->getMessage(); //debug
                throw new Exception('Could not connect database.', $e->getCode());
            }
        }
    }
    
    /**
     * 返回原始pdo实例
     * @return PDO
     */
    public function getPDOInstance() {
        $this->connect();
        return $this->_db;
    }
    
    // ------------------------ PDO -------------------------------//

    /**
     * PDO::quote
     * @param string $string
     * @return string
     */
    public function quote($string) {
        return $this->_db->quote($string);
    }

    /**
     * PDO::exec
     * @param type $sql
     * @return bool
     */
    public function exec($sql) {
        $this->debug($sql);
        return $this->_db->exec($sql);
    }

    /**
     * PDO query
     * @param type $sql
     * @return PDOStatement
     */
    public function query($sql) {
        $this->debug($sql);
        return $this->_db->query($sql);
    }
    
    // ------------------------ PDOStatement -------------------------------//

    /**
     * 执行sql语句 PDOStatement::execute
     * @param type $sql
     * @param type $bind_array
     * @param type $unbuffered
     * @return bool
     */
    public function executeSql($sql, $bind_array = array()) {
        $this->debug($sql, $bind_array);
        $sth = $this->_db->prepare($sql);
        if ($bind_array) {
            $ret = $sth->execute($bind_array);
        } else {
            $ret = $sth->execute();
        }
        return $ret;
    }
    
    // --------------------- wrapper method -----------------------------//

    /**
     * 插入
     * example: $id = insert('message', $field_value, true);
     */
    public function insert($table, $data, $return_insert_id = true, $replace = false) {
        $cmd = $replace ? 'REPLACE INTO' : 'INSERT INTO';

        if (!$data) {
            return false;
        }
        $tmp = $this->implode($data);
        $place_sql = $tmp['place'];
        $bind_array = $tmp['bind'];

        $sql = "$cmd $table SET $place_sql";

        $ret = $this->executeSql($sql, $bind_array);
        if ($ret && $return_insert_id) {
            return $this->_db->lastInsertId();
        }
        return $ret;
    }

    /**
     * 更新
     * example: update('message', $field_value, "id=40");
     * example: update('message', $field_value, array('id'=>40));
     */
    public function update($table, $data, $condition, $low_priority = false, $return_row_count = false) {
        if (!$data) {
            return false;
        }
        $tmp = $this->implode($data);
        if (empty($tmp['place']) || empty($tmp['bind'])) {
            return false;
        }
        $place_sql = $tmp['place'];
        $bind_array = $tmp['bind'];
        $cmd = "UPDATE " . ($low_priority ? 'LOW_PRIORITY' : '');
        $where = '';
        if (empty($condition)) {
            $where = '1 = 1';
        } elseif (is_array($condition)) {
            $wt = $this->implode($condition, ' AND ');
            $where = $wt['place'];
            $bind_array = array_merge($bind_array, $wt['bind']);
        } else {
            $where = $condition;
        }

        $sql = "$cmd $table SET $place_sql WHERE $where";

        $this->debug($sql, $bind_array);

        $sth = $this->_db->prepare($sql);
        if ($bind_array) {
            $ret = $sth->execute($bind_array);
        } else {
            $ret = $sth->execute();
        }
        
        if ($ret && $return_row_count) {
            return $sth->rowCount();
        }
        return $ret;
    }

    /**
     * example delete('message', array('id'=>22))
     * 删除
     */
    public function delete($table, $condition, $bind_array = array(), $limit = 0, $return_row_count = false) {
        if (empty($condition)) {
            return false;
        }
        $cmd = 'DELETE FROM';
        if (is_array($condition)) {
            $tmp = $this->implode($condition, 'AND');
            $where = $tmp['place'];
            $bind_array = $tmp['bind'];
        } else {
            $where = $condition;
        }
        $limit_str = $limit > 0 ? "LIMIT $limit" : '';
        $sql = "$cmd $table WHERE $where $limit_str";

        $this->debug($sql, $bind_array);
        $sth = $this->_db->prepare($sql);

        if ($bind_array) {
            $ret = $sth->execute($bind_array);
        } else {
            $ret = $sth->execute();
        }
        
        if ($ret && $return_row_count) {
            return $sth->rowCount();
        }
        return $ret;
    }

    /**
     * 获取一个单元格
     * example:
     * $sql = "select * from message where id>? limit 3";
     * fetchOne($sql, array(35));
     * Return first row first field
     */
    public function fetchOne($sql, $bind_array = array()) {
        $this->debug($sql, $bind_array);
        $sth = $this->_db->prepare($sql);
        if ($bind_array) {
            $sth->execute($bind_array);
        } else {
            $sth->execute();
        }
        return $sth->fetchColumn(0);
    }

    /**
     * 获取第一行
     * Return First Row
     */
    public function fetchRow($sql, $bind_array = array()) {
        $this->debug($sql, $bind_array);
        $sth = $this->_db->prepare($sql);
        $sth->execute($bind_array);
        return $sth->fetch(PDO::FETCH_ASSOC);
    }

    /**
     * 获取列
     * example: fetchByColumn($sql, array(35), 'id');
     * @return array
     */
    public function fetchColumn($sql, $bind_array = array(), $index = 0) {
        $this->debug($sql, $bind_array);
        $sth = $this->_db->prepare($sql);
        $sth->execute($bind_array);
        
        $data = array();
        if (is_numeric($index)) {
            while ($r = $sth->fetchColumn($index)) {
                $data[] = $r;
            }
        } else {
            while ($r = $sth->fetch(PDO::FETCH_ASSOC)) {
                $data[] = $r[$index];
            }
        }
        return $data;
    }

    /**
     * 获取多行
     * example:
     * $sql = "select * from message where id>? ";
     * fetchAll($sql, array(39));
     */
    public function fetchAll($sql, $bind_array = array(), $limit_start = 0, $limit_step = 0, $use_buffered = true) {
        $limit_step = intval($limit_step);
        if ($limit_step > 0) {
            $tmp = intval($limit_start);
            $limit_start = $tmp ? $tmp : 0;
            $sql .= " LIMIT $limit_start, $limit_step";
        }

        $this->debug($sql, $bind_array);
        $sth = $this->_db->prepare($sql, array(PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => $use_buffered));
        $sth->execute($bind_array);
        return $sth->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * 占位符
     * @param string $field
     * @return string
     */
    public function placeholder($field) {
        $field = trim($field, '`');
        return "`$field`=?";
    }

    /**
     * 
     * @param type $array
     * @param string $glue
     * @return array
     */
    public function implode($array, $glue = ',') {
        $bind = array();
        $sql = $comma = '';
        $glue = ' ' . trim($glue) . ' ';
        foreach ($array as $k => $v) {
            $sql .= $comma . $this->placeholder($k);
            $comma = $glue;
            $bind[] = (is_null($v) ? '' : $v);
        }
        return array('place' => $sql, 'bind' => $bind);
    }
    
    // ---------------- schema --------------- //
    /**
     * 列出所有表格
     * @return array
     */
    public function showTables($dbname) {
        $ret = array();
        $this->exec("use $dbname");
        $tmp = $this->fetchAll('SHOW TABLES');
        if ($tmp) {
            foreach ($tmp as $row) {
                $k = 'Tables_in_' . $dbname;
                $ret[] = $row[$k];
            }
        }
        return $ret;
    }
    
    /**
     * 表格字段
     * @param type $tableName
     * @return array
     */
    public function showFields($tableName) {
        $sql = 'SHOW COLUMNS FROM ' . $tableName;
        $result =   $this->fetchAll($sql);
        $info   =   array();
        if($result) {
            foreach ($result as $key => $val) {
                $info[$val['Field']] = array(
                    'name'    => $val['Field'],
                    'type'    => $val['Type'],
                    'notnull' => (bool) (strtoupper($val['Null']) === 'NO'), // not null is empty, null is yes
                    'default' => $val['Default'],
                    'primary' => (strtolower($val['Key']) == 'pri'),
                    'autoinc' => (strtolower($val['Extra']) == 'auto_increment'),
                );
            }
        }
        return $info;
    }
    
    // ---------------- error info --------------- //
    
    /**
     * Fetches the SQLSTATE associated with the last database operation.
     * @return integer The last error code.
     */
    public function errorCode() {
        return $this->_db->errorCode();
    }
    
    /**
     * Fetches extended error information associated with the last database operation.
     * @return array The last error information.
     */
    public function errorInfo() {
        return $this->_conn->errorInfo();
    }
    
    // ---------------- logging --------------- //
    
    /**
     * 记录日志
     * @param string $sql
     * @param array $params
     */
    protected function debug($sql, $params = '') {
        $str = 'SQL: ' . $sql;
        if ($params) {
            if (is_array($params)) {
                $str .= "\nParam: " . var_export($params, true);
            } else {
                $str .= $params;
            }
        }
        $this->_debug_log[] = $str;
    }
    
    /**
     * 获取执行记录
     * @return type
     */
    public function getLog() {
        return $this->_debug_log;
    }
    
}