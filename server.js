
import express from 'express';
import mysql from 'mysql2/promise';
import cors from 'cors';
import bodyParser from 'body-parser';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3004;

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static(__dirname));

// Database connection configuration
const dbConfig = {
  host: '192.171.1.10',
  user: 'hikuser',
  password: 'Hope@2025!',
  database: 'agliculture_att',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
};

// Create connection pool
let db;
const initDB = async () => {
  try {
    db = mysql.createPool(dbConfig);
    const connection = await db.getConnection();
    console.log('✅ Connected to MySQL database: agliculture_att');
    connection.release();
    return db;
  } catch (err) {
    console.error('❌ Error connecting to MySQL database:', err);
    console.log('Please check your database credentials and network connection.');
    process.exit(1);
  }
};

// ========== FAMILY FEES OVERVIEW ENDPOINT ==========
app.get('/api/school-fees/families', async (req, res) => {
  try {
    // Query to get only real families (father-mother pairs who share at least one child)
    const query = `
      SELECT
        CONCAT('F', COALESCE(father.ParentID, 'NULL'), '_M', COALESCE(mother.ParentID, 'NULL')) AS family_id,
        father.ParentID AS father_id,
        father.FullName AS father_name,
        mother.ParentID AS mother_id,
        mother.FullName AS mother_name,
        COUNT(DISTINCT s.StudentID) AS kids_count,
        COALESCE(SUM(CASE WHEN father.ParentID IS NOT NULL AND sfp.amount_paid IS NOT NULL AND spFather.ParentID = father.ParentID THEN sfp.amount_paid ELSE 0 END), 0) AS father_fee_credit,
        COALESCE(SUM(CASE WHEN mother.ParentID IS NOT NULL AND sfp.amount_paid IS NOT NULL AND spMother.ParentID = mother.ParentID THEN sfp.amount_paid ELSE 0 END), 0) AS mother_fee_credit,
        COALESCE(SUM(sfp.amount_paid), 0) AS total_fee_credit,
        GROUP_CONCAT(DISTINCT CONCAT(s.Registration_Number, ' - ', s.FirstName, ' ', s.LastName, ' (', s.Class, ')') SEPARATOR ', ') AS children_combined
      FROM Students s
      LEFT JOIN StudentParent spFather ON spFather.StudentID = s.StudentID
      LEFT JOIN Parents father ON father.ParentID = spFather.ParentID AND father.Gender = 'Male'
      LEFT JOIN StudentParent spMother ON spMother.StudentID = s.StudentID
      LEFT JOIN Parents mother ON mother.ParentID = spMother.ParentID AND mother.Gender = 'Female'
      LEFT JOIN student_fee_payments sfp ON sfp.student_id = s.StudentID
      GROUP BY father.ParentID, mother.ParentID
      ORDER BY father.ParentID, mother.ParentID
      LIMIT 100
    `;
    const families = await dbQuery(query);
    res.json({ families });
  } catch (err) {
    console.error('Error fetching family fees:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});
// Create student_fee_payments table if not exists
const createFeesTable = async () => {
  try {
    // Create the table with all required columns including amount_paid
    await dbQuery(`
      CREATE TABLE IF NOT EXISTS student_fee_payments (
        id INT AUTO_INCREMENT PRIMARY KEY,
        student_id INT,
        amount DECIMAL(10,2) DEFAULT 0,
        amount_paid DECIMAL(10,2) DEFAULT 0,
        payment_date DATE,
        term VARCHAR(50),
        year INT,
        payment_method VARCHAR(50),
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (student_id) REFERENCES Students(StudentID)
      )
    `);
    console.log('✅ student_fee_payments table ready');

    const columnCheck = await dbGet(
      `SELECT COUNT(*) AS column_exists
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = ?
         AND TABLE_NAME = 'student_fee_payments'
         AND COLUMN_NAME = 'amount_paid'`,
      [dbConfig.database]
    );

    if (Number(columnCheck?.column_exists || 0) === 0) {
      await dbQuery('ALTER TABLE student_fee_payments ADD COLUMN amount_paid DECIMAL(10,2) DEFAULT 0 AFTER amount');
      console.log('✅ Added amount_paid column to student_fee_payments');
    } else {
      console.log('✅ amount_paid column already exists');
    }
  } catch (err) {
    console.error('Error creating fees table:', err);
  }
};

// Helper function to execute MySQL queries
const dbQuery = async (query, params = []) => {
  if (!db) {
    console.error('Database not initialized');
    throw new Error('Database connection not established');
  }
  try {
    const [rows] = await db.query(query, params);
    return rows;
  } catch (err) {
    console.error('Database query error:', err);
    throw err;
  }
};

// Helper function for single row queries
const dbGet = async (query, params = []) => {
  if (!db) {
    console.error('Database not initialized');
    throw new Error('Database connection not established');
  }
  try {
    const [rows] = await db.execute(query, params);
    return rows[0] || null;
  } catch (err) {
    console.error('Database get error:', err);
    throw err;
  }
};

// Helper function for run operations (INSERT, UPDATE, DELETE)
const dbRun = async (query, params = []) => {
  if (!db) {
    console.error('Database not initialized');
    throw new Error('Database connection not established');
  }
  try {
    const [result] = await db.execute(query, params);
    return {
      lastID: result.insertId,
      changes: result.affectedRows
    };
  } catch (err) {
    console.error('Database run error:', err);
    throw err;
  }
};

// Check for essential tables on startup
const checkTables = async () => {
  try {
    const tables = await dbQuery("SHOW TABLES");
    const tableNames = tables.map(row => Object.values(row)[0]);
    console.log('Available tables:', tableNames);

    // Check for essential tables based on your database
    const essentialTables = ['Parents', 'Students', 'StudentParent', 'AccessLogs', 'shifts', 'parent_rates', 'devices'];
    const optionalTables = ['v_attendance_completed', 'v_attendance_with_rates', 'v_parent_daily_totals', 'v_shift_events'];

    console.log('\n🔍 Essential Tables:');
    for (const table of essentialTables) {
      const exists = tableNames.includes(table);
      console.log(`  ${table}: ${exists ? '✅' : '❌'}`);
    }

    console.log('\n🔍 Optional Tables/Views:');
    for (const table of optionalTables) {
      const exists = tableNames.includes(table);
      console.log(`  ${table}: ${exists ? '✅' : '❌ (optional)'}`);
    }
  } catch (err) {
    console.error('Error checking tables:', err);
  }
};

// ========== LOGIN ENDPOINTS ==========

app.post('/api/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    const normalizedEmail = (email || '').trim().toLowerCase();
    const normalizedPassword = (password || '').trim();
    
    if (!normalizedEmail || !normalizedPassword) {
      return res.status(400).json({ error: 'Email and password are required' });
    }

    const user = await dbGet(
      'SELECT id, email, name, role, is_active FROM users WHERE LOWER(email) = ? AND password = ? AND is_active = 1',
      [normalizedEmail, normalizedPassword]
    );

    if (!user) {
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    // Preserve expected route behavior when role is missing/generic in DB.
    let role = (user.role || '').toLowerCase();
    if (!role || role === 'user') {
      if (normalizedEmail.includes('money@')) role = 'money';
      else if (normalizedEmail.includes('admin@')) role = 'admin';
      else if (normalizedEmail.includes('shift@')) role = 'shift';
      else if (normalizedEmail.includes('fees@')) role = 'fees';
      else role = 'user';
    }

    const userResponse = {
      id: user.id,
      email: user.email || normalizedEmail,
      name: user.name || normalizedEmail.split('@')[0],
      role: role
    };

    res.json({
      message: 'Login successful',
      user: userResponse,
      token: 'authenticated_' + Date.now()
    });

  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({ error: 'Server error during login' });
  }
});

// GET CURRENT USER
app.get('/api/current-user', async (req, res) => {
  try {
    const email = req.query.email;
    const normalizedEmail = (email || '').trim().toLowerCase();
    if (!email) {
      return res.status(400).json({ error: 'Email is required' });
    }

    // Determine role based on email
    let role = 'user';
    if (normalizedEmail.includes('money@')) role = 'money';
    else if (normalizedEmail.includes('admin@')) role = 'admin';
    else if (normalizedEmail.includes('shift@')) role = 'shift';
    else if (normalizedEmail.includes('fees@')) role = 'fees';

    res.json({
      email: normalizedEmail,
      name: normalizedEmail.split('@')[0],
      role: role
    });

  } catch (err) {
    console.error('Get user error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// ========== PARENTS MANAGEMENT ==========

// Get all parents
app.get('/api/parents', async (req, res) => {
  try {
    const { search } = req.query;
    
    let query = `SELECT ParentID, FullName, PhoneNumber, 
      CASE 
        WHEN PhoneNumber IS NULL OR PhoneNumber = '' THEN ''
        WHEN MomoVerify IS NOT NULL AND MomoVerify != '' THEN MomoVerify
        ELSE ''
      END AS MomoVerify,
      Gender, NumberOfKids FROM Parents WHERE 1=1`;
    const params = [];
    
    if (search) {
      query += ' AND (FullName LIKE ? OR PhoneNumber LIKE ? OR ParentID LIKE ?)';
      const searchTerm = `%${search}%`;
      params.push(searchTerm, searchTerm, searchTerm);
    }
    
    query += ' ORDER BY FullName';
    const parents = await dbQuery(query, params);
    // Map Gender to Role for frontend
    const parentsWithRole = parents.map(p => ({
      ...p,
      MomoVerify: p.MomoVerify || (p.PhoneNumber ? '' : ''),
      Role: p.Gender === 'Male' ? 'Father' : p.Gender === 'Female' ? 'Mother' : ''
    }));
    res.json(parentsWithRole);
  } catch (err) {
    console.error('Error fetching parents:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Add new parent
app.post('/api/parents', async (req, res) => {
  try {
    const { FullName, PhoneNumber, MomoVerify, Gender, NumberOfKids } = req.body;
    const result = await dbRun(
      'INSERT INTO Parents (FullName, PhoneNumber, MomoVerify, Gender, NumberOfKids) VALUES (?, ?, ?, ?, ?)',
      [FullName, PhoneNumber, MomoVerify || '', Gender, NumberOfKids]
    );
    res.json({ 
      message: 'Parent added successfully', 
      id: result.lastID 
    });
  } catch (err) {
    console.error('Error adding parent:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Update parent
app.put('/api/parents/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { FullName, PhoneNumber, MomoVerify, Gender, NumberOfKids } = req.body;
    const result = await dbRun(
      'UPDATE Parents SET FullName = ?, PhoneNumber = ?, MomoVerify = ?, Gender = ?, NumberOfKids = ? WHERE ParentID = ?',
      [FullName, PhoneNumber, MomoVerify || '', Gender, NumberOfKids, id]
    );
    
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Parent not found' });
    }
    
    res.json({ message: 'Parent updated successfully' });
  } catch (err) {
    console.error('Error updating parent:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Delete parent
app.delete('/api/parents/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await dbRun(
      'DELETE FROM Parents WHERE ParentID = ?',
      [id]
    );
    
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Parent not found' });
    }
    
    res.json({ message: 'Parent deleted successfully' });
  } catch (err) {
    console.error('Error deleting parent:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Get parent details
app.get('/api/parents/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const parent = await dbGet(`
      SELECT
        p.*,
        pr.MoneyRateOverrideRWF,
        pr.updated_at as rate_updated_at
      FROM Parents p
      LEFT JOIN parent_rates pr ON p.ParentID = pr.ParentID
      WHERE p.ParentID = ?
    `, [id]);

    if (!parent) {
      return res.status(404).json({ error: 'Parent not found' });
    }

    let shifts = [];
    let kids = [];

    try {
      // Get shift information for this parent (with fallback if view doesn't exist)
      const shiftsQuery = `
        SELECT
          v.AccessDate,
          v.shift_name,
          v.start_time,
          v.end_time,
          v.check_in_time,
          v.check_out_time,
          CASE
            WHEN v.device_type_for_calc = 'money' THEN 'money'
            WHEN v.device_type_for_calc = 'fees' THEN 'fees'
            ELSE 'other'
          END as shift_type,
          v.money_earned_rwf,
          v.fee_earned_rwf,
          (v.money_earned_rwf + v.fee_earned_rwf) as total_earned
        FROM v_attendance_with_rates v
        WHERE v.ParentID = ?
        ORDER BY v.AccessDate DESC, v.start_time
        LIMIT 10
      `;

      shifts = await dbQuery(shiftsQuery, [id]);
      // Remove duplicate/invalid shifts (same parent, shift name, check-in, check-out)
      const seen = new Set();
      shifts = shifts.filter(shift => {
        const key = `${id}_${shift.shift_name}_${shift.check_in_time}_${shift.check_out_time}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    } catch (shiftError) {
      console.warn('Could not fetch shift data for parent:', shiftError.message);
      // Fallback: get basic attendance data from AccessLogs
      try {
        const fallbackShiftsQuery = `
          SELECT
            DATE(AccessDateTime) as AccessDate,
            'Unknown' as shift_name,
            TIME(AccessDateTime) as check_in_time,
            TIME(AccessDateTime) as check_out_time,
            CASE
              WHEN DeviceName = 'Gukorera Amafaranga' OR DeviceName LIKE '%Money%' THEN 'money'
              WHEN DeviceName = 'Gukorea Abana' OR DeviceName LIKE '%Fee%' THEN 'fees'
              ELSE 'other'
            END as shift_type,
            CASE
              WHEN DeviceName = 'Gukorera Amafaranga' OR DeviceName LIKE '%Money%' THEN 2500
              ELSE 0
            END as money_earned_rwf,
            CASE
              WHEN DeviceName = 'Gukorea Abana' OR DeviceName LIKE '%Fee%' THEN 2000
              ELSE 0
            END as fee_earned_rwf,
            CASE
              WHEN DeviceName = 'Gukorera Amafaranga' OR DeviceName LIKE '%Money%' THEN 2500
              WHEN DeviceName = 'Gukorea Abana' OR DeviceName LIKE '%Fee%' THEN 2000
              ELSE 0
            END as total_earned
          FROM AccessLogs
          WHERE ParentID = ?
          ORDER BY AccessDateTime DESC
          LIMIT 10
        `;
        shifts = await dbQuery(fallbackShiftsQuery, [id]);
      } catch (fallbackError) {
        console.warn('Could not fetch fallback shift data:', fallbackError.message);
        shifts = [];
      }
    }

    try {
      // Get kids' names for this parent
      const kidsQuery = `
        SELECT
          s.StudentID,
          s.FirstName,
          s.LastName,
          s.Registration_Number,
          s.Class,
          sp.Relationship
        FROM Students s
        JOIN StudentParent sp ON s.StudentID = sp.StudentID
        WHERE sp.ParentID = ?
        ORDER BY s.LastName, s.FirstName
      `;

      kids = await dbQuery(kidsQuery, [id]);
    } catch (kidsError) {
      console.warn('Could not fetch kids data for parent:', kidsError.message);
      kids = [];
    }

    // Format the response with additional information
    const enhancedParent = {
      ...parent,
      shifts: shifts.map(shift => ({
        date: shift.AccessDate,
        shift_name: shift.shift_name || 'Unknown',
        start_time: shift.start_time,
        end_time: shift.end_time,
        check_in_time: shift.check_in_time,
        check_out_time: shift.check_out_time,
        shift_type: shift.shift_type,
        money_earned: shift.money_earned_rwf || 0,
        fees_earned: shift.fee_earned_rwf || 0,
        total_earned: shift.total_earned || 0
      })),
      kids: kids.map(kid => ({
        student_id: kid.StudentID,
        full_name: `${kid.FirstName} ${kid.LastName}`,
        registration_number: kid.Registration_Number,
        class: kid.Class,
        relationship: kid.Relationship
      }))
    };

    res.json(enhancedParent);
  } catch (err) {
    console.error('Error fetching parent details:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// ========== STUDENTS MANAGEMENT ==========

// Get all students
app.get('/api/students', async (req, res) => {
  try {
    const { search } = req.query;

    let query = 'SELECT * FROM Students WHERE 1=1';
    const params = [];

    if (search) {
      const term = `%${String(search).trim()}%`;
      query += `
        AND (
          CAST(StudentID AS CHAR) LIKE ?
          OR Registration_Number LIKE ?
          OR FirstName LIKE ?
          OR LastName LIKE ?
          OR CONCAT(FirstName, ' ', LastName) LIKE ?
        )
      `;
      params.push(term, term, term, term, term);
    }

    query += ' ORDER BY StudentID DESC';

    const students = await dbQuery(query, params);
    res.json(students);
  } catch (err) {
    console.error('Error fetching students:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Add new student
app.post('/api/students', async (req, res) => {
  try {
    const { Registration_Number, LastName, FirstName, Class } = req.body;
    const result = await dbRun(
      'INSERT INTO Students (Registration_Number, LastName, FirstName, Class) VALUES (?, ?, ?, ?)',
      [Registration_Number, LastName, FirstName, Class]
    );
    res.json({ 
      message: 'Student added successfully', 
      id: result.lastID 
    });
  } catch (err) {
    console.error('Error adding student:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Update student
app.put('/api/students/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { Registration_Number, LastName, FirstName, Class } = req.body;
    const result = await dbRun(
      'UPDATE Students SET Registration_Number = ?, LastName = ?, FirstName = ?, Class = ? WHERE StudentID = ?',
      [Registration_Number, LastName, FirstName, Class, id]
    );
    
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Student not found' });
    }
    
    res.json({ message: 'Student updated successfully' });
  } catch (err) {
    console.error('Error updating student:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Delete student
app.delete('/api/students/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await dbRun(
      'DELETE FROM Students WHERE StudentID = ?',
      [id]
    );
    
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Student not found' });
    }
    
    res.json({ message: 'Student deleted successfully' });
  } catch (err) {
    console.error('Error deleting student:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// ========== STUDENT-PARENT RELATIONS ==========

// Get all relations (student-parent)
app.get('/api/relations', async (req, res) => {
  try {
    const { studentId, parentId, studentName, parentName } = req.query;

    let query = `
      SELECT 
        sp.StudentID,
        sp.ParentID,
        sp.Relationship,
        s.Registration_Number,
        s.FirstName as StudentFirstName,
        s.LastName as StudentLastName,
        s.Class as StudentClass,
        p.FullName as ParentFullName
      FROM StudentParent sp
      JOIN Students s ON sp.StudentID = s.StudentID
      JOIN Parents p ON sp.ParentID = p.ParentID
      WHERE 1=1
    `;

    const params = [];

    if (studentId) {
      query += ' AND sp.StudentID = ?';
      params.push(studentId);
    }

    if (parentId) {
      query += ' AND sp.ParentID = ?';
      params.push(parentId);
    }

    if (studentName) {
      query += ' AND (s.FirstName LIKE ? OR s.LastName LIKE ?)';
      params.push(`%${studentName}%`, `%${studentName}%`);
    }

    if (parentName) {
      query += ' AND p.FullName LIKE ?';
      params.push(`%${parentName}%`);
    }

    query += ' ORDER BY s.LastName, s.FirstName';

    const results = await dbQuery(query, params);
    res.json(results);
  } catch (err) {
    console.error('Error fetching relations:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Add new relation
app.post('/api/relations', async (req, res) => {
  try {
    const { StudentID, ParentID, Relationship } = req.body;
    const result = await dbRun(
      'INSERT INTO StudentParent (StudentID, ParentID, Relationship) VALUES (?, ?, ?)',
      [StudentID, ParentID, Relationship]
    );
    res.json({ message: 'Relation added successfully' });
  } catch (err) {
    console.error('Error adding relation:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Update relation
app.put('/api/relations/:studentId/:parentId', async (req, res) => {
  try {
    const { studentId, parentId } = req.params;
    const { Relationship } = req.body;
    const result = await dbRun(
      'UPDATE StudentParent SET Relationship = ? WHERE StudentID = ? AND ParentID = ?',
      [Relationship, studentId, parentId]
    );
    
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Relation not found' });
    }
    
    res.json({ message: 'Relation updated successfully' });
  } catch (err) {
    console.error('Error updating relation:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Delete relation
app.delete('/api/relations/:studentId/:parentId', async (req, res) => {
  try {
    const { studentId, parentId } = req.params;
    const result = await dbRun(
      'DELETE FROM StudentParent WHERE StudentID = ? AND ParentID = ?',
      [studentId, parentId]
    );
    
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Relation not found' });
    }
    
    res.json({ message: 'Relation deleted successfully' });
  } catch (err) {
    console.error('Error deleting relation:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// ========== SHIFTS MANAGEMENT ==========

// Get all shifts
app.get('/api/shifts', async (req, res) => {
  try {
    const shifts = await dbQuery(`
      SELECT * FROM shifts 
      WHERE status = 1 
      ORDER BY start_time
    `);
    res.json(shifts);
  } catch (err) {
    console.error('Error fetching shifts:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Get single shift
app.get('/api/shifts/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const shift = await dbGet('SELECT * FROM shifts WHERE shift_id = ?', [id]);
    
    if (!shift) {
      return res.status(404).json({ error: 'Shift not found' });
    }
    
    res.json(shift);
  } catch (err) {
    console.error('Error fetching shift:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Add new shift
app.post('/api/shifts', async (req, res) => {
  try {
    const { 
      shift_name, 
      start_time, 
      end_time, 
      checkin_start, 
      checkin_end, 
      checkout_start, 
      checkout_end,
      money_rate_rwf,
      fee_rate_rwf,
      money_rate_rwf_special,
      is_special_shift,
      status = 1
    } = req.body;

    const result = await dbRun(
      `INSERT INTO shifts (
        shift_name, start_time, end_time, checkin_start, checkin_end, 
        checkout_start, checkout_end, money_rate_rwf, fee_rate_rwf,
        money_rate_rwf_special, is_special_shift, status, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())`,
      [
        shift_name, start_time, end_time, checkin_start, checkin_end,
        checkout_start, checkout_end, money_rate_rwf, fee_rate_rwf,
        money_rate_rwf_special, is_special_shift, status
      ]
    );

    res.json({ 
      message: 'Shift added successfully', 
      id: result.lastID 
    });
  } catch (err) {
    console.error('Error adding shift:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Update shift
app.put('/api/shifts/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;
    
    // Build dynamic update query
    const fields = [];
    const values = [];
    
    for (const [key, value] of Object.entries(updateData)) {
      if (key !== 'id' && key !== 'shift_id') {
        fields.push(`${key} = ?`);
        values.push(value);
      }
    }
    
    if (fields.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }
    
    values.push(id);
    
    const query = `UPDATE shifts SET ${fields.join(', ')} WHERE shift_id = ?`;
    const result = await dbRun(query, values);
    
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Shift not found' });
    }
    
    res.json({ message: 'Shift updated successfully' });
  } catch (err) {
    console.error('Error updating shift:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Delete shift
app.delete('/api/shifts/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await dbRun(
      'DELETE FROM shifts WHERE shift_id = ?',
      [id]
    );

    if (result.changes === 0) {
      return res.status(404).json({ error: 'Shift not found' });
    }

    res.json({ message: 'Shift deleted successfully' });
  } catch (err) {
    console.error('Error deleting shift:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Get shifts for filters
app.get('/api/shifts/filters', async (req, res) => {
  try {
    const shifts = await dbQuery(`
      SELECT shift_id, shift_name, is_special_shift 
      FROM shifts 
      WHERE status = 1 
      ORDER BY shift_name
    `);
    res.json(shifts);
  } catch (err) {
    console.error('Error fetching shifts for filters:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Get student shifts
app.get('/api/student-shifts/:studentId', async (req, res) => {
  try {
    const { studentId } = req.params;
    const { startDate, endDate } = req.query;
    
    // Get the parent associated with this student
    const parentQuery = `
      SELECT p.*, sp.Relationship 
      FROM StudentParent sp
      JOIN Parents p ON sp.ParentID = p.ParentID
      WHERE sp.StudentID = ?
      LIMIT 1
    `;
    
    const parentResult = await dbQuery(parentQuery, [studentId]);
    
    if (parentResult.length === 0) {
      return res.json({
        student: null,
        parent: null,
        shifts: []
      });
    }
    
    const parent = parentResult[0];
    
    // Get student information
    const studentQuery = `SELECT * FROM Students WHERE StudentID = ?`;
    const studentResult = await dbQuery(studentQuery, [studentId]);
    const student = studentResult[0];
    
    // Get shifts for the parent from v_attendance_with_rates
    let shiftsQuery = `
      SELECT 
        v.*,
        s.shift_name,
        s.start_time,
        s.end_time,
        s.money_rate_rwf,
        s.fee_rate_rwf,
        s.money_rate_rwf_special,
        s.is_special_shift
      FROM v_attendance_with_rates v
      LEFT JOIN shifts s ON v.shift_id = s.shift_id
      WHERE v.ParentID = ?
    `;
    
    const params = [parent.ParentID];
    
    if (startDate && endDate) {
      shiftsQuery += ' AND v.AccessDate BETWEEN ? AND ?';
      params.push(startDate, endDate);
    }
    
    shiftsQuery += ' ORDER BY v.AccessDate DESC, v.shift_name';
    
    const shifts = await dbQuery(shiftsQuery, params);
    
    // Group shifts by day
    const shiftsByDay = {};
    shifts.forEach(shift => {
      const date = shift.AccessDate;
      if (!shiftsByDay[date]) {
        shiftsByDay[date] = [];
      }
      shiftsByDay[date].push(shift);
    });
    
    res.json({
      student: student,
      parent: parent,
      shifts: shifts,
      shiftsByDay: shiftsByDay,
      totalShifts: shifts.length,
      totalDays: Object.keys(shiftsByDay).length,
      summary: {
        totalEarnings: shifts.reduce((sum, shift) => sum + (shift.money_earned_rwf || 0) + (shift.fee_earned_rwf || 0), 0),
        totalMoney: shifts.reduce((sum, shift) => sum + (shift.money_earned_rwf || 0), 0),
        totalFees: shifts.reduce((sum, shift) => sum + (shift.fee_earned_rwf || 0), 0)
      }
    });
  } catch (err) {
    console.error('Error fetching student shifts:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== PARENT RATES MANAGEMENT ==========

// Get parent custom rates
app.get('/api/parent-rates', async (req, res) => {
  try {
    const { parentId, includeParents = false } = req.query;
    
    let query = `
      SELECT pr.*, p.FullName, p.PhoneNumber, p.NumberOfKids
      FROM parent_rates pr
      JOIN Parents p ON pr.ParentID = p.ParentID
      WHERE 1=1
    `;
    
    const params = [];
    
    if (parentId) {
      query += ' AND pr.ParentID = ?';
      params.push(parentId);
    }
    
    query += ' ORDER BY p.FullName';
    
    const rates = await dbQuery(query, params);
    
    // If includeParents flag is true, also return parents without custom rates
    if (includeParents) {
      const allParents = await dbQuery(`
        SELECT ParentID, FullName, PhoneNumber, NumberOfKids 
        FROM Parents 
        ORDER BY FullName
      `);
      
      const parentsWithRates = new Set(rates.map(r => r.ParentID));
      const parentsWithoutRates = allParents.filter(p => !parentsWithRates.has(p.ParentID));
      
      res.json({
        custom_rates: rates,
        parents_without_rates: parentsWithoutRates.map(p => ({
          ...p,
          MoneyRateOverrideRWF: null,
          note: 'No custom rate set',
          created_at: null,
          updated_at: null
        })),
        summary: {
          total_parents: allParents.length,
          parents_with_custom_rates: rates.length,
          parents_without_custom_rates: parentsWithoutRates.length
        }
      });
    } else {
      res.json(rates);
    }
  } catch (err) {
    console.error('Error fetching parent rates:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// Set or update parent custom rate
app.post('/api/parent-rates', async (req, res) => {
  try {
    const { ParentID, MoneyRateOverrideRWF, note } = req.body;
    
    if (!ParentID || !MoneyRateOverrideRWF) {
      return res.status(400).json({ error: 'ParentID and MoneyRateOverrideRWF are required' });
    }
    
    // Check if parent exists
    const parent = await dbGet('SELECT ParentID FROM Parents WHERE ParentID = ?', [ParentID]);
    if (!parent) {
      return res.status(404).json({ error: 'Parent not found' });
    }
    
    
    const existingRate = await dbGet('SELECT * FROM parent_rates WHERE ParentID = ?', [ParentID]);
    
    if (existingRate) {
      // Update existing rate
      const result = await dbRun(
        `UPDATE parent_rates SET 
          MoneyRateOverrideRWF = ?, 
          note = ?, 
          updated_at = NOW() 
        WHERE ParentID = ?`,
        [MoneyRateOverrideRWF, note, ParentID]
      );
      
      res.json({ 
        message: 'Parent rate updated successfully',
        updated: true 
      });
    } else {
      // Insert new rate
      const result = await dbRun(
        `INSERT INTO parent_rates (ParentID, MoneyRateOverrideRWF, note, created_at, updated_at) 
         VALUES (?, ?, ?, NOW(), NOW())`,
        [ParentID, MoneyRateOverrideRWF, note]
      );
      
      res.json({ 
        message: 'Parent rate added successfully',
        id: result.lastID,
        updated: false 
      });
    }
  } catch (err) {
    console.error('Error setting parent rate:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// Delete parent custom rate
app.delete('/api/parent-rates/:parentId', async (req, res) => {
  try {
    const { parentId } = req.params;
    
    const result = await dbRun(
      'DELETE FROM parent_rates WHERE ParentID = ?',
      [parentId]
    );
    
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Parent rate not found' });
    }
    
    res.json({ message: 'Parent rate deleted successfully' });
  } catch (err) {
    console.error('Error deleting parent rate:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== COMPREHENSIVE EARNERS API ==========

// GET COMPREHENSIVE EARNERS DATA
app.get('/api/comprehensive-earners', async (req, res) => {
  try {
    // Query to get all parents with their earnings data from agliculture_att database
    const query = `
      SELECT
        p.ParentID,
        p.FullName as parent_name,
        p.PhoneNumber as phone,
        COUNT(al.LogID) as shifts,
        COUNT(DISTINCT DATE(al.AccessDateTime)) as days_worked,
        COALESCE(SUM(CASE WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 2500 ELSE 0 END), 0) as total_money,
        COALESCE(SUM(CASE WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 2000 ELSE 0 END), 0) as total_fees,
        COALESCE(SUM(
          CASE WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 2500
               WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 2000
               ELSE 0 END
        ), 0) as total_earnings,
        COALESCE(ROUND(SUM(
          CASE WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 2500
               WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 2000
               ELSE 0 END
        ) / NULLIF(COUNT(al.LogID), 0), 0), 0) as avg_per_shift,
        COALESCE(pr.MoneyRateOverrideRWF, 2000) as custom_rate
      FROM Parents p
      LEFT JOIN AccessLogs al ON p.ParentID = al.ParentID
      LEFT JOIN parent_rates pr ON p.ParentID = pr.ParentID
      GROUP BY p.ParentID, p.FullName, p.PhoneNumber
      HAVING total_earnings > 0 AND avg_per_shift >= 2500
      ORDER BY total_earnings DESC
      LIMIT 100
    `;

    const results = await dbQuery(query);

    // Filter for parents earning exactly 2500 per shift on average
    const exact2500Earners = results.filter(r => r.avg_per_shift === 2500);

    // Check for parents with high earnings (>= 2500 avg)
    const highEarners = results.filter(r => r.avg_per_shift >= 2500);

    // Summary statistics
    const totalEarnings = results.reduce((sum, r) => sum + r.total_earnings, 0);
    const avgEarnings = results.length > 0 ? Math.round(totalEarnings / results.length) : 0;

    const responseData = {
      summary: {
        totalParents: results.length,
        totalEarnings: totalEarnings,
        averageEarnings: avgEarnings,
        exact2500Earners: exact2500Earners.length,
        highEarners: highEarners.length
      },
      exact2500Earners: exact2500Earners.map(earner => ({
        parentId: earner.ParentID,
        parentName: earner.parent_name,
        phone: earner.phone,
        shifts: earner.shifts,
        daysWorked: earner.days_worked,
        totalMoney: earner.total_money,
        totalFees: earner.total_fees,
        totalEarnings: earner.total_earnings,
        avgPerShift: earner.avg_per_shift,
        customRate: earner.custom_rate
      })),
      top10Earners: results.slice(0, 10).map(earner => ({
        parentId: earner.ParentID,
        parentName: earner.parent_name,
        totalEarnings: earner.total_earnings,
        avgPerShift: earner.avg_per_shift,
        shifts: earner.shifts
      })),
      highEarners: highEarners.map(earner => ({
        parentName: earner.parent_name,
        avgPerShift: earner.avg_per_shift,
        totalEarnings: earner.total_earnings,
        shifts: earner.shifts
      })),
      allEarners: results.map(earner => ({
        parentId: earner.ParentID,
        parentName: earner.parent_name,
        phone: earner.phone,
        shifts: earner.shifts,
        daysWorked: earner.days_worked,
        totalMoney: earner.total_money,
        totalFees: earner.total_fees,
        totalEarnings: earner.total_earnings,
        avgPerShift: earner.avg_per_shift,
        customRate: earner.custom_rate
      }))
    };

    res.json(responseData);
  } catch (err) {
    console.error('Error fetching comprehensive earners:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== TOP EARNERS API ==========

// GET TOP EARNERS DASHBOARD DATA - FIXED VERSION
app.get('/api/top-earners/dashboard', async (req, res) => {
  try {
    const {
      startDate = '2025-10-01',
      endDate = '2025-10-31',
      minRate = 2000,
      limit = 50,
      search = ''
    } = req.query;

    const effectiveMinRate = parseInt(minRate) || 2000;
    const effectiveLimit = parseInt(limit) || 50;
    const effectiveSearch = String(search || '').trim();

    console.log(`Fetching top earners dashboard with params:`, {
      startDate, endDate, minRate: effectiveMinRate, limit: effectiveLimit, search: effectiveSearch
    });

    // Prefer view-based aggregation that is already deduplicated per parent/day.
    let query;
    let params;
    let queryType = 'fallback';

    try {
      const parentTotalsViewCheck = await dbQuery("SHOW TABLES LIKE 'v_parent_daily_totals'");
      const parentTotalsViewExists = parentTotalsViewCheck.length > 0;

      if (parentTotalsViewExists) {
        queryType = 'view_parent_daily_totals';
        // This returns one row per parent and prevents duplicates in top earners.
        query = `
          SELECT
            p.ParentID,
            p.FullName,
            p.PhoneNumber,
            p.Gender,
            p.NumberOfKids,
            t.total_money as total_money,
            t.total_fees as total_fees,
            t.total_earnings as total_earnings,
            t.days_worked as days_worked,
            t.shifts_completed as shifts_completed,
            t.avg_earnings_per_day as avg_earnings_per_day,
            t.avg_per_shift as avg_per_shift,
            COALESCE(pr.MoneyRateOverrideRWF, 0) as custom_rate,
            COALESCE(
              pr.MoneyRateOverrideRWF,
              (SELECT MAX(COALESCE(s2.money_rate_rwf_special, s2.money_rate_rwf, 2000)) FROM shifts s2 WHERE s2.status = 1),
              2000
            ) as effective_rate,
            CASE
              WHEN COALESCE(
                pr.MoneyRateOverrideRWF,
                (SELECT MAX(COALESCE(s3.money_rate_rwf_special, s3.money_rate_rwf, 2000)) FROM shifts s3 WHERE s3.status = 1),
                2000
              ) >= ? THEN 'High Earner'
              ELSE 'Standard Earner'
            END as earner_category
          FROM Parents p
          JOIN (
            SELECT
              ParentID,
              COALESCE(SUM(total_money_rwf), 0) as total_money,
              COALESCE(SUM(total_fee_rwf), 0) as total_fees,
              COALESCE(SUM(total_money_rwf + total_fee_rwf), 0) as total_earnings,
              COUNT(DISTINCT AccessDate) as days_worked,
              COALESCE(SUM(shifts_completed), 0) as shifts_completed,
              COALESCE(ROUND(SUM(total_money_rwf + total_fee_rwf) / NULLIF(COUNT(DISTINCT AccessDate), 0), 0), 0) as avg_earnings_per_day,
              COALESCE(ROUND(SUM(total_money_rwf + total_fee_rwf) / NULLIF(SUM(shifts_completed), 0), 0), 0) as avg_per_shift
            FROM v_parent_daily_totals
            WHERE AccessDate BETWEEN ? AND ?
            GROUP BY ParentID
          ) t ON p.ParentID = t.ParentID
          LEFT JOIN parent_rates pr ON p.ParentID = pr.ParentID
          WHERE 1=1
          ${effectiveSearch ? 'AND (CAST(p.ParentID AS CHAR) LIKE ? OR p.FullName LIKE ?)' : ''}
          AND t.total_earnings > 0
          AND t.avg_per_shift >= ?
          ORDER BY total_earnings DESC, avg_per_shift DESC
          LIMIT ?
        `;

        params = [
          effectiveMinRate, // For CASE statement
          startDate, endDate, // Date range
          ...(effectiveSearch ? [`%${effectiveSearch}%`, `%${effectiveSearch}%`] : []),
          effectiveMinRate,
          effectiveLimit
        ];
      } else {
        // Fallback to simpler query using AccessLogs directly
        console.log('v_parent_daily_totals view not found, using fallback query');
        query = `
          SELECT
            p.ParentID,
            p.FullName,
            p.PhoneNumber,
            p.Gender,
            p.NumberOfKids,
            COALESCE(SUM(
              CASE 
                WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 2500
                WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 2000
                ELSE 0
              END
            ), 0) as total_earnings,
            COUNT(DISTINCT DATE(al.AccessDateTime)) as days_worked,
            COUNT(*) as shifts_completed,
            COALESCE(ROUND(SUM(
              CASE 
                WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 2500
                WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 2000
                ELSE 0
              END
            ) / NULLIF(COUNT(*), 0), 0), 0) as avg_per_shift,
            COALESCE(pr.MoneyRateOverrideRWF, 0) as custom_rate,
            COALESCE(pr.MoneyRateOverrideRWF, 2000) as effective_rate,
            CASE
              WHEN pr.MoneyRateOverrideRWF >= ? THEN 'High Earner'
              ELSE 'Standard Earner'
            END as earner_category,
            SUM(CASE WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 2500 ELSE 0 END) as total_money,
            SUM(CASE WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 2000 ELSE 0 END) as total_fees
          FROM Parents p
          LEFT JOIN AccessLogs al ON p.ParentID = al.ParentID
            AND DATE(al.AccessDateTime) BETWEEN ? AND ?
          LEFT JOIN parent_rates pr ON p.ParentID = pr.ParentID
          WHERE 1=1
          ${effectiveSearch ? 'AND (CAST(p.ParentID AS CHAR) LIKE ? OR p.FullName LIKE ?)' : ''}
          GROUP BY p.ParentID, p.FullName, p.PhoneNumber, p.Gender, p.NumberOfKids, pr.MoneyRateOverrideRWF
          HAVING total_earnings > 0 AND (avg_per_shift >= ? OR custom_rate >= ?)
          ORDER BY total_earnings DESC, avg_per_shift DESC
          LIMIT ?
        `;

        params = [
          effectiveMinRate, // For CASE statement
          startDate, endDate, // Date range
          ...(effectiveSearch ? [`%${effectiveSearch}%`, `%${effectiveSearch}%`] : []),
          effectiveMinRate, effectiveMinRate, // HAVING conditions
          effectiveLimit
        ];
      }

      console.log('Executing query:', query.substring(0, 200) + '...');
      const results = await dbQuery(query, params);

      // Calculate summary statistics
      const totalEarnings = results.reduce((sum, row) => sum + (Number(row.total_earnings) || 0), 0);
      const summary = {
        totalTopEarners: results.length,
        totalEarnings: totalEarnings,
        avgEarningsPerTopEarner: results.length > 0 ?
          Math.round(totalEarnings / results.length) : 0,
        highEarnersCount: results.filter(r => r.earner_category === 'High Earner').length,
        highestEarning: results.length > 0 ? (Number(results[0].total_earnings) || 0) : 0,
        highestAvgPerShift: results.length > 0 ? (Number(results[0].avg_per_shift) || 0) : 0
      };

      console.log('Results summary:', summary);

      res.json({
        topEarners: results,
        summary: summary,
        filters: {
          startDate: startDate,
          endDate: endDate,
          minRate: effectiveMinRate,
          limit: effectiveLimit
        },
        queryType: queryType
      });

    } catch (queryError) {
      console.error('Query error:', queryError);
      
      // Return a simplified response if the query fails
      res.json({
        topEarners: [],
        summary: {
          totalTopEarners: 0,
          totalEarnings: 0,
          avgEarningsPerTopEarner: 0,
          highEarnersCount: 0,
          highestEarning: 0,
          highestAvgPerShift: 0
        },
        filters: {
          startDate: startDate,
          endDate: endDate,
          minRate: effectiveMinRate,
          limit: effectiveLimit
        },
        error: 'Query execution failed',
        message: queryError.message
      });
    }

  } catch (err) {
    console.error('Error fetching top earners dashboard:', err);
    res.status(500).json({ 
      error: 'Database error',
      message: err.message,
      stack: err.stack 
    });
  }
});

// Simple test endpoint
app.get('/api/test-top-earners', async (req, res) => {
  try {
    const results = await dbQuery(`
      SELECT 
        p.ParentID,
        p.FullName,
        COUNT(*) as shift_count,
        COALESCE(pr.MoneyRateOverrideRWF, 0) as custom_rate
      FROM Parents p
      LEFT JOIN AccessLogs al ON p.ParentID = al.ParentID
      LEFT JOIN parent_rates pr ON p.ParentID = pr.ParentID
      WHERE DATE(al.AccessDateTime) BETWEEN '2025-10-01' AND '2025-10-31'
      GROUP BY p.ParentID, p.FullName, pr.MoneyRateOverrideRWF
      HAVING shift_count > 0
      ORDER BY shift_count DESC
      LIMIT 10
    `);
    
    res.json({
      success: true,
      data: results,
      count: results.length
    });
    
  } catch (err) {
    console.error('Test endpoint error:', err);
    res.status(500).json({ 
      success: false,
      error: err.message 
    });
  }
});

// GET TOP EARNERS BY CUSTOM RATE
app.get('/api/top-earners/custom-rate', async (req, res) => {
  try {
    const { minRate = 2500 } = req.query;

    const query = `
      SELECT 
        p.ParentID,
        p.FullName,
        p.PhoneNumber,
        p.NumberOfKids,
        pr.MoneyRateOverrideRWF as custom_rate,
        pr.updated_at as rate_updated,
        COALESCE(SUM(v.money_earned_rwf), 0) as total_money,
        COALESCE(SUM(v.fee_earned_rwf), 0) as total_fees,
        COALESCE(SUM(v.money_earned_rwf + v.fee_earned_rwf), 0) as total_earnings,
        COUNT(DISTINCT v.AccessDate) as days_worked,
        COUNT(*) as shifts_completed
      FROM parent_rates pr
      JOIN Parents p ON pr.ParentID = p.ParentID
      LEFT JOIN v_attendance_with_rates v ON p.ParentID = v.ParentID
      WHERE pr.MoneyRateOverrideRWF >= ?
      GROUP BY p.ParentID, p.FullName, p.PhoneNumber, p.NumberOfKids, 
               pr.MoneyRateOverrideRWF, pr.updated_at
      ORDER BY pr.MoneyRateOverrideRWF DESC, total_earnings DESC
    `;

    const results = await dbQuery(query, [minRate]);

    res.json({
      customRateEarners: results,
      summary: {
        total: results.length,
        highestCustomRate: results.length > 0 ? results[0].custom_rate : 0,
        avgCustomRate: results.length > 0 ? 
          Math.round(results.reduce((sum, row) => sum + row.custom_rate, 0) / results.length) : 0
      }
    });

  } catch (err) {
    console.error('Error fetching custom rate earners:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// GET SHIFT-BASED TOP EARNERS
app.get('/api/top-earners/by-shift', async (req, res) => {
  try {
    const { shiftType = 'special', minRate = 2500 } = req.query;

    const query = `
      SELECT 
        p.ParentID,
        p.FullName,
        s.shift_name,
        s.money_rate_rwf as base_rate,
        s.money_rate_rwf_special as special_rate,
        CASE 
          WHEN s.is_special_shift = 1 THEN s.money_rate_rwf_special
          ELSE s.money_rate_rwf
        END as effective_rate,
        COUNT(*) as shifts_worked,
        COALESCE(SUM(v.money_earned_rwf), 0) as total_money,
        COALESCE(SUM(v.fee_earned_rwf), 0) as total_fees,
        COALESCE(SUM(v.money_earned_rwf + v.fee_earned_rwf), 0) as total_earnings
      FROM Parents p
      JOIN v_attendance_with_rates v ON p.ParentID = v.ParentID
      JOIN shifts s ON v.shift_id = s.shift_id
      WHERE (
        (s.is_special_shift = 1 AND s.money_rate_rwf_special >= ?)
        OR (s.is_special_shift = 0 AND s.money_rate_rwf >= ?)
      )
      GROUP BY p.ParentID, p.FullName, s.shift_name, s.money_rate_rwf, 
               s.money_rate_rwf_special, s.is_special_shift
      HAVING effective_rate >= ?
      ORDER BY effective_rate DESC, total_earnings DESC
    `;

    const results = await dbQuery(query, [minRate, minRate, minRate]);

    res.json({
      shiftBasedEarners: results,
      summary: {
        total: results.length,
        uniqueShifts: [...new Set(results.map(r => r.shift_name))].length,
        highestRate: results.length > 0 ? results[0].effective_rate : 0
      }
    });

  } catch (err) {
    console.error('Error fetching shift-based earners:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// GET TOP EARNER DETAILED SHIFT DATA (FOR VIEW CARD)
app.get('/api/top-earners/:parentId/details', async (req, res) => {
  try {
    const { parentId } = req.params;
    const {
      startDate = '2025-10-01',
      endDate = '2025-10-31'
    } = req.query;

    const parent = await dbGet(
      `
        SELECT
          p.ParentID,
          p.FullName,
          p.PhoneNumber,
          p.Gender,
          p.NumberOfKids
        FROM Parents p
        WHERE p.ParentID = ?
      `,
      [parentId]
    );

    if (!parent) {
      return res.status(404).json({ error: 'Parent not found' });
    }

    let shifts = [];

    try {
      const detailedShiftQuery = `
        SELECT
          DATE(v.AccessDate) as AccessDate,
          v.shift_name,
          s.start_time,
          s.end_time,
          v.check_in_time,
          v.check_out_time,
          CASE
            WHEN v.device_type_for_calc = 'money' THEN 'money'
            WHEN v.device_type_for_calc = 'fees' THEN 'fees'
            ELSE 'other'
          END as shift_type,
          COALESCE(v.money_earned_rwf, 0) as money_earned,
          COALESCE(v.fee_earned_rwf, 0) as fees_earned,
          COALESCE(v.money_earned_rwf, 0) + COALESCE(v.fee_earned_rwf, 0) as total_earned
        FROM v_attendance_with_rates v
        LEFT JOIN shifts s ON v.shift_id = s.shift_id
        WHERE v.ParentID = ?
          AND DATE(v.AccessDate) BETWEEN ? AND ?
        ORDER BY DATE(v.AccessDate) DESC, s.start_time, v.check_in_time
      `;

      shifts = await dbQuery(detailedShiftQuery, [parentId, startDate, endDate]);
    } catch (shiftError) {
      console.warn('Could not fetch detailed shifts from view:', shiftError.message);

      const fallbackShiftQuery = `
        SELECT
          DATE(al.AccessDateTime) as AccessDate,
          'Unknown' as shift_name,
          NULL as start_time,
          NULL as end_time,
          TIME(al.AccessDateTime) as check_in_time,
          TIME(al.AccessDateTime) as check_out_time,
          CASE
            WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 'money'
            WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 'fees'
            ELSE 'other'
          END as shift_type,
          CASE
            WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 2500
            ELSE 0
          END as money_earned,
          CASE
            WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 2000
            ELSE 0
          END as fees_earned,
          CASE
            WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 2500
            WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 2000
            ELSE 0
          END as total_earned
        FROM AccessLogs al
        WHERE al.ParentID = ?
          AND DATE(al.AccessDateTime) BETWEEN ? AND ?
        ORDER BY DATE(al.AccessDateTime) DESC, TIME(al.AccessDateTime)
      `;

      shifts = await dbQuery(fallbackShiftQuery, [parentId, startDate, endDate]);
    }

    const normalizedShifts = shifts.map((shift) => ({
      date: shift.AccessDate,
      shift_name: shift.shift_name || 'Unknown',
      start_time: shift.start_time,
      end_time: shift.end_time,
      check_in_time: shift.check_in_time,
      check_out_time: shift.check_out_time,
      shift_type: shift.shift_type || 'other',
      money_earned: Number(shift.money_earned) || 0,
      fees_earned: Number(shift.fees_earned) || 0,
      total_earned: Number(shift.total_earned) || 0
    }));

    const shiftsByDayMap = normalizedShifts.reduce((acc, shift) => {
      const dateKey = shift.date;
      if (!acc[dateKey]) {
        acc[dateKey] = {
          date: dateKey,
          totalShifts: 0,
          completedShifts: 0,
          shifts: []
        };
      }

      acc[dateKey].totalShifts += 1;
      if (shift.check_in_time && shift.check_out_time) {
        acc[dateKey].completedShifts += 1;
      }
      acc[dateKey].shifts.push(shift);
      return acc;
    }, {});

    const shiftsByDay = Object.values(shiftsByDayMap).sort((a, b) => {
      return String(b.date).localeCompare(String(a.date));
    });

    const totals = {
      totalShiftsDone: normalizedShifts.length,
      completedShifts: normalizedShifts.filter((s) => s.check_in_time && s.check_out_time).length,
      totalDaysWorked: shiftsByDay.length,
      totalMoney: normalizedShifts.reduce((sum, s) => sum + s.money_earned, 0),
      totalFees: normalizedShifts.reduce((sum, s) => sum + s.fees_earned, 0),
      totalEarnings: normalizedShifts.reduce((sum, s) => sum + s.total_earned, 0)
    };

    res.json({
      parent,
      filters: {
        startDate,
        endDate
      },
      totals,
      shiftsByDay,
      shifts: normalizedShifts
    });
  } catch (err) {
    console.error('Error fetching top earner details:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// UPDATE CUSTOM RATE FOR PARENT
app.put('/api/top-earners/:parentId/rate', async (req, res) => {
  try {
    const { parentId } = req.params;
    const { MoneyRateOverrideRWF } = req.body;

    // Check if parent exists
    const parent = await dbGet('SELECT * FROM Parents WHERE ParentID = ?', [parentId]);
    if (!parent) {
      return res.status(404).json({ error: 'Parent not found' });
    }

    // Check if rate already exists
    const existingRate = await dbGet(
      'SELECT * FROM parent_rates WHERE ParentID = ?',
      [parentId]
    );

    let result;
    if (existingRate) {
      // Update existing rate
      result = await dbRun(
        'UPDATE parent_rates SET MoneyRateOverrideRWF = ?, updated_at = NOW() WHERE ParentID = ?',
        [MoneyRateOverrideRWF, parentId]
      );
    } else {
      // Insert new rate
      result = await dbRun(
        'INSERT INTO parent_rates (ParentID, MoneyRateOverrideRWF, created_at, updated_at) VALUES (?, ?, NOW(), NOW())',
        [parentId, MoneyRateOverrideRWF]
      );
    }

    res.json({
      success: true,
      message: 'Rate updated successfully',
      parentId: parentId,
      MoneyRateOverrideRWF: MoneyRateOverrideRWF,
      updatedAt: new Date().toISOString()
    });

  } catch (err) {
    console.error('Error updating rate:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// DELETE CUSTOM RATE FOR TOP EARNERS
app.delete('/api/top-earners/:parentId/rate', async (req, res) => {
  try {
    const { parentId } = req.params;
    
    const result = await dbRun(
      'DELETE FROM parent_rates WHERE ParentID = ?',
      [parentId]
    );
    
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Rate not found' });
    }
    
    res.json({ 
      success: true,
      message: 'Rate deleted successfully' 
    });
  } catch (err) {
    console.error('Error deleting rate:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// ========== PARENT EARNINGS API ==========

// PARENT EARNINGS COMPARISON
app.get('/api/parent-earnings/comparison', async (req, res) => {
  try {
    const { startDate = '2025-10-01', endDate = '2025-10-31' } = req.query;

    const query = `
      SELECT
        p.ParentID,
        p.FullName,
        p.PhoneNumber,
        p.Gender,
        p.NumberOfKids,
        COALESCE(SUM(v.money_earned_rwf), 0) as total_money,
        COALESCE(SUM(v.fee_earned_rwf), 0) as total_fees,
        COALESCE(SUM(v.money_earned_rwf + v.fee_earned_rwf), 0) as total_earnings,
        COUNT(DISTINCT v.AccessDate) as days_worked,
        COUNT(*) as shifts_completed,
        COALESCE(ROUND(SUM(v.money_earned_rwf + v.fee_earned_rwf) / NULLIF(COUNT(DISTINCT v.AccessDate), 0), 0), 0) as avg_earnings_per_day,
        COALESCE(ROUND(SUM(v.money_earned_rwf + v.fee_earned_rwf) / NULLIF(COUNT(*), 0), 0), 0) as avg_per_shift,
        COALESCE(pr.MoneyRateOverrideRWF, 0) as custom_rate
      FROM Parents p
      LEFT JOIN v_attendance_with_rates v ON p.ParentID = v.ParentID AND v.AccessDate BETWEEN ? AND ?
      LEFT JOIN parent_rates pr ON p.ParentID = pr.ParentID
      GROUP BY p.ParentID, p.FullName, p.PhoneNumber, p.Gender, p.NumberOfKids
      HAVING total_earnings > 0
      ORDER BY total_earnings DESC
      LIMIT 50
    `;

    const results = await dbQuery(query, [startDate, endDate]);

    // Add ranking and categorization
    const dataWithRanking = results.map((row, index) => {
      let earnings_category = 'Low Earner';
      if (row.total_earnings >= 10000) earnings_category = 'High Earner';
      else if (row.total_earnings >= 5000) earnings_category = 'Medium Earner';

      return {
        ...row,
        earnings_rank: index + 1,
        earnings_category: earnings_category,
        attendance_rank: index + 1 // Simplified, could be based on attendance percentage
      };
    });

    // Calculate statistics
    const statistics = {
      total_earnings: dataWithRanking.reduce((sum, row) => sum + row.total_earnings, 0),
      avg_earnings_per_parent: dataWithRanking.length > 0 ?
        Math.round(dataWithRanking.reduce((sum, row) => sum + row.total_earnings, 0) / dataWithRanking.length) : 0,
      top_earner: dataWithRanking.length > 0 ? {
        name: dataWithRanking[0].FullName,
        earnings: dataWithRanking[0].total_earnings
      } : null,
      earnings_distribution: {
        high_earners: dataWithRanking.filter(r => r.earnings_category === 'High Earner').length,
        medium_earners: dataWithRanking.filter(r => r.earnings_category === 'Medium Earner').length,
        low_earners: dataWithRanking.filter(r => r.earnings_category === 'Low Earner').length
      }
    };

    res.json({
      data: dataWithRanking,
      statistics: statistics,
      period: {
        startDate: startDate,
        endDate: endDate
      }
    });
  } catch (err) {
    console.error('Error fetching parent earnings comparison:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// PARENT EARNINGS TRENDS
app.get('/api/parent-earnings/trends', async (req, res) => {
  try {
    const { year = '2025', period = 'monthly', parentId } = req.query;

    let dateFormat = '';
    let groupBy = '';
    let periodLabel = '';

    if (period === 'monthly') {
      dateFormat = 'DATE_FORMAT(v.AccessDate, "%Y-%m")';
      groupBy = 'DATE_FORMAT(v.AccessDate, "%Y-%m")';
      periodLabel = 'DATE_FORMAT(v.AccessDate, "%Y-%m")';
    } else if (period === 'weekly') {
      dateFormat = 'DATE_FORMAT(v.AccessDate, "%Y-W%U")';
      groupBy = 'DATE_FORMAT(v.AccessDate, "%Y-W%U")';
      periodLabel = 'CONCAT(DATE_FORMAT(v.AccessDate, "%Y"), "-W", DATE_FORMAT(v.AccessDate, "%U"))';
    } else if (period === 'daily') {
      dateFormat = 'DATE(v.AccessDate)';
      groupBy = 'DATE(v.AccessDate)';
      periodLabel = 'DATE(v.AccessDate)';
    }

    let whereClause = `WHERE YEAR(v.AccessDate) = ?`;
    const params = [year];

    if (parentId) {
      whereClause += ' AND v.ParentID = ?';
      params.push(parentId);
    }

    const query = `
      SELECT
        ${periodLabel} as period_label,
        ${dateFormat} as period,
        COUNT(DISTINCT v.ParentID) as parents_count,
        COUNT(DISTINCT v.AccessDate) as days_worked,
        COUNT(*) as shifts_completed,
        COALESCE(SUM(v.money_earned_rwf), 0) as total_money,
        COALESCE(SUM(v.fee_earned_rwf), 0) as total_fees,
        COALESCE(SUM(v.money_earned_rwf + v.fee_earned_rwf), 0) as total_earnings,
        COALESCE(ROUND(AVG(v.money_earned_rwf + v.fee_earned_rwf), 0), 0) as avg_per_shift,
        COALESCE(ROUND(SUM(v.money_earned_rwf + v.fee_earned_rwf) / NULLIF(COUNT(DISTINCT v.AccessDate), 0), 0), 0) as avg_per_day
      FROM v_attendance_with_rates v
      ${whereClause}
      GROUP BY ${groupBy}
      ORDER BY period
    `;

    const results = await dbQuery(query, params);

    // Calculate growth percentages
    const dataWithGrowth = results.map((row, index) => {
      let growth_percentage = 0;
      if (index > 0) {
        const prevEarnings = results[index - 1].total_earnings;
        if (prevEarnings > 0) {
          growth_percentage = Math.round(((row.total_earnings - prevEarnings) / prevEarnings) * 100);
        }
      }
      return {
        ...row,
        growth_percentage: growth_percentage
      };
    });

    res.json({
      data: dataWithGrowth,
      period: period,
      year: year,
      summary: {
        total_periods: dataWithGrowth.length,
        total_earnings: dataWithGrowth.reduce((sum, row) => sum + row.total_earnings, 0),
        total_shifts: dataWithGrowth.reduce((sum, row) => sum + row.shifts_completed, 0),
        avg_earnings_per_period: dataWithGrowth.length > 0 ?
          Math.round(dataWithGrowth.reduce((sum, row) => sum + row.total_earnings, 0) / dataWithGrowth.length) : 0
      }
    });
  } catch (err) {
    console.error('Error fetching parent earnings trends:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// PARENT EARNINGS SUMMARY
app.get('/api/parent-earnings/summary', async (req, res) => {
  try {
    const { startDate = '2025-10-01', endDate = '2025-10-31', shiftId, search = '' } = req.query;

    console.log(`📊 /api/parent-earnings/summary: ${startDate} to ${endDate}`);

    let whereClause = 'WHERE DATE(al.AccessDateTime) BETWEEN ? AND ?';
    const params = [startDate, endDate];
    const normalizedSearch = String(search || '').trim();

    if (shiftId) {
      whereClause += ' AND EXISTS (SELECT 1 FROM shifts WHERE shift_id = ?)';
      params.push(shiftId);
    }

    if (normalizedSearch) {
      whereClause += ' AND (CAST(p.ParentID AS CHAR) LIKE ? OR p.FullName LIKE ?)';
      params.push(`%${normalizedSearch}%`, `%${normalizedSearch}%`);
    }

    let results = [];
    let queryType = 'fallback';

    // Use v_attendance_with_rates view for real money/fees and support startDate/endDate
    // Only count the first check-in and last check-out per parent per day
    const viewQuery = `
      SELECT
        v.ParentID, p.FullName, p.PhoneNumber, p.Gender, p.NumberOfKids,
        COALESCE(SUM(v.money_earned_rwf), 0) as total_money,
        COALESCE(SUM(v.fee_earned_rwf), 0) as total_fees,
        COALESCE(SUM(v.money_earned_rwf + v.fee_earned_rwf), 0) as total_earnings,
        COUNT(DISTINCT v.AccessDate) as days_worked,
        COUNT(*) as shifts_completed,
        COALESCE(ROUND(SUM(v.money_earned_rwf + v.fee_earned_rwf) / NULLIF(COUNT(DISTINCT v.AccessDate), 0), 0), 0) as avg_earnings_per_day
      FROM (
        SELECT * FROM v_attendance_with_rates v1
        WHERE (v1.check_in_time, v1.ParentID, v1.AccessDate) IN (
          SELECT MIN(v2.check_in_time), v2.ParentID, v2.AccessDate
          FROM v_attendance_with_rates v2
          WHERE v2.AccessDate BETWEEN ? AND ?
            AND v2.check_in_time != v2.check_out_time
            AND v2.device_type_for_calc IN ('money','fees')
          GROUP BY v2.ParentID, v2.AccessDate
        )
        OR (v1.check_out_time, v1.ParentID, v1.AccessDate) IN (
          SELECT MAX(v3.check_out_time), v3.ParentID, v3.AccessDate
          FROM v_attendance_with_rates v3
          WHERE v3.AccessDate BETWEEN ? AND ?
            AND v3.check_in_time != v3.check_out_time
            AND v3.device_type_for_calc IN ('money','fees')
          GROUP BY v3.ParentID, v3.AccessDate
        )
      ) v
      LEFT JOIN Parents p ON v.ParentID = p.ParentID
      WHERE v.AccessDate BETWEEN ? AND ?
      ${normalizedSearch ? 'AND (CAST(v.ParentID AS CHAR) LIKE ? OR p.FullName LIKE ?)' : ''}
      GROUP BY v.ParentID, p.FullName, p.PhoneNumber, p.Gender, p.NumberOfKids
      HAVING (
        (COALESCE(SUM(v.money_earned_rwf), 0) > 0 AND COALESCE(SUM(v.fee_earned_rwf), 0) = 0)
        OR
        (COALESCE(SUM(v.money_earned_rwf), 0) = 0 AND COALESCE(SUM(v.fee_earned_rwf), 0) > 0)
      )
      ORDER BY total_earnings DESC
    `;
    const viewParams = [startDate, endDate, startDate, endDate, startDate, endDate];
    if (normalizedSearch) viewParams.push(`%${normalizedSearch}%`, `%${normalizedSearch}%`);
    results = await dbQuery(viewQuery, viewParams);
    queryType = 'v_attendance_with_rates_first_last_only';

    console.log(`✅ Summary query success (${queryType}): ${results.length} parents`);
    res.json(results);
  } catch (err) {
    console.error('❌ Critical error in parent-earnings/summary:', err);
    res.status(500).json({ error: 'Server error', details: err.message });
  }
});

// DETAILED PARENT SHIFTS
app.get('/api/parent-earnings/detailed', async (req, res) => {
  try {
    const { parentId, startDate = '2025-10-01', endDate = '2025-10-31' } = req.query;

    // If parentId is provided, return detailed data for that specific parent
    if (parentId) {
      // Get parent info
      const parent = await dbGet('SELECT * FROM Parents WHERE ParentID = ?', [parentId]);
      if (!parent) {
        return res.status(404).json({ error: 'Parent not found' });
      }

      // Get daily totals with shift details
      const dailyData = await dbQuery(`
        SELECT
          v.AccessDate,
          v.shift_name,
          v.check_in_time,
          v.check_out_time,
          v.money_earned_rwf,
          v.fee_earned_rwf,
          v.device_type_for_calc,
          v.is_special_shift
        FROM v_attendance_with_rates v
        WHERE v.ParentID = ? AND v.AccessDate BETWEEN ? AND ?
        ORDER BY v.AccessDate DESC, v.check_in_time
      `, [parentId, startDate, endDate]);

      // Group by date
      const groupedData = {};
      dailyData.forEach(row => {
        const date = row.AccessDate;
        if (!groupedData[date]) {
          groupedData[date] = {
            AccessDate: date,
            total_money: 0,
            total_fees: 0,
            total_earnings: 0,
            shifts: []
          };
        }

        groupedData[date].shifts.push({
          shift_name: row.shift_name,
          check_in: row.check_in_time,
          check_out: row.check_out_time,
          money_earned: row.money_earned_rwf,
          fee_earned: row.fee_earned_rwf,
          total: row.money_earned_rwf + row.fee_earned_rwf,
          device_type: row.device_type_for_calc,
          is_special: row.is_special_shift
        });

        groupedData[date].total_money += row.money_earned_rwf;
        groupedData[date].total_fees += row.fee_earned_rwf;
        groupedData[date].total_earnings += row.money_earned_rwf + row.fee_earned_rwf;
      });

      const detailedData = Object.values(groupedData);

      res.json({
        parent: parent,
        data: detailedData,
        summary: {
          total_shifts: dailyData.length,
          total_money: detailedData.reduce((sum, day) => sum + day.total_money, 0),
          total_fees: detailedData.reduce((sum, day) => sum + day.total_fees, 0),
          total_earnings: detailedData.reduce((sum, day) => sum + day.total_earnings, 0)
        }
      });
    } else {
      // If the range is a single day, return daily breakdown for all parents
      // Always return daily breakdown for all parents in the range
      const earnings = await dbQuery(`
        SELECT
          p.ParentID,
          p.FullName,
          p.PhoneNumber,
          p.MomoVerify,
          DATE(al.AccessDateTime) as AccessDate,
          COUNT(CASE WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' OR al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 1 END) as shifts,
          COUNT(CASE WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 1 END) * 2500 as total_money,
          COUNT(CASE WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 1 END) * 2000 as total_fees
        FROM Parents p
        LEFT JOIN AttendanceLog al ON p.ParentID = al.ParentID AND DATE(al.AccessDateTime) BETWEEN ? AND ?
        GROUP BY p.ParentID, p.FullName, p.PhoneNumber, p.MomoVerify, DATE(al.AccessDateTime)
        HAVING total_money > 0 OR total_fees > 0
        ORDER BY AccessDate DESC, p.FullName
      `, [startDate, endDate]);
      const data = earnings.map(row => ({
        ...row,
        total_earnings: Number(row.total_money || 0) + Number(row.total_fees || 0)
      }));
      if (data.length === 0) {
        console.log('⚠️ No data for range - returning empty');
        res.json({ data: [], summary: { total_rows: 0, total_earnings: 0, note: 'No earnings in date range' } });
        return;
      }
      res.json({
        data: data,
        summary: {
          total_rows: data.length,
          total_earnings: data.reduce((sum, row) => sum + (row.total_earnings || 0), 0)
        }
      });
    }
  } catch (err) {
    console.error('Error fetching detailed parent earnings:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== MONEY TOTALS API ==========

app.get('/api/money-totals', async (req, res) => {
  try {
    const { 
      startDate = '2025-10-01', 
      endDate = '2025-10-31',
      minAmount,
      maxAmount,
      shiftId
    } = req.query;
    
    let whereClause = 'WHERE v.AccessDate BETWEEN ? AND ?';
    const params = [startDate, endDate];
    
    if (shiftId) {
      whereClause += ' AND v.shift_id = ?';
      params.push(shiftId);
    }
    
    if (minAmount) {
      whereClause += ' AND (v.money_earned_rwf + v.fee_earned_rwf) >= ?';
      params.push(parseInt(minAmount));
    }
    
    if (maxAmount) {
      whereClause += ' AND (v.money_earned_rwf + v.fee_earned_rwf) <= ?';
      params.push(parseInt(maxAmount));
    }
    
    const query = `
      SELECT
        p.ParentID,
        p.FullName as parent_name,
        v.shift_name,
        v.AccessDate as date,
        v.money_earned_rwf,
        v.fee_earned_rwf,
        (v.money_earned_rwf + v.fee_earned_rwf) as total,
        v.device_type_for_calc as device_type,
        CASE 
          WHEN v.money_earned_rwf > 0 OR v.fee_earned_rwf > 0 THEN 'Earned'
          ELSE 'No Earnings'
        END as status
      FROM v_attendance_with_rates v
      JOIN Parents p ON v.ParentID = p.ParentID
      ${whereClause}
      ORDER BY v.AccessDate DESC, total DESC
      LIMIT 500
    `;
    
    const results = await dbQuery(query, params);
    
    // Calculate totals
    const totals = results.reduce((acc, row) => {
      acc.totalMoney += row.money_earned_rwf || 0;
      acc.totalFees += row.fee_earned_rwf || 0;
      acc.totalAmount += row.total || 0;
      return acc;
    }, { totalMoney: 0, totalFees: 0, totalAmount: 0 });
    
    res.json({
      records: results,
      totals: totals,
      summary: {
        recordCount: results.length,
        moneyParents: new Set(results.filter(r => r.money_earned_rwf > 0).map(r => r.parent_name)).size,
        feesParents: new Set(results.filter(r => r.fee_earned_rwf > 0).map(r => r.parent_name)).size
      }
    });
  } catch (err) {
    console.error('Error fetching money totals:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== ATTENDANCE STATISTICS ==========

app.get('/api/attendance-stats', async (req, res) => {
  try {
    const { startDate = '2025-10-01', endDate = '2025-10-31', device } = req.query;

    let baseCondition = `WHERE DATE(AccessDateTime) BETWEEN ? AND ?`;
    const params = [startDate, endDate];

    if (device) {
      baseCondition += ` AND DeviceName LIKE ?`;
      params.push(`%${device}%`);
    }

    const totalEntries = await dbQuery(`SELECT COUNT(*) as total FROM AccessLogs ${baseCondition}`, params);

    // Get today's entries
    const today = new Date().toISOString().split('T')[0];
    const todayParams = [today];
    if (device) {
      todayParams.push(`%${device}%`);
    }

    const todayEntries = await dbQuery(
      `SELECT COUNT(*) as today FROM AccessLogs WHERE DATE(AccessDateTime) = ? ${device ? 'AND DeviceName LIKE ?' : ''}`,
      todayParams
    );

    // Get present count based on AuthenticationResult
    const presentCount = await dbQuery(
      `SELECT COUNT(*) as present FROM AccessLogs ${baseCondition} AND (AuthenticationResult LIKE '%Pass%' OR AuthenticationResult LIKE '%HumanDetect%')`,
      params
    );

    const deviceStats = await dbQuery(
      `SELECT DeviceName, COUNT(*) as count FROM AccessLogs ${baseCondition} GROUP BY DeviceName`,
      params
    );

    // Determine attendance status based on AuthenticationResult
    const statusStats = await dbQuery(`
      SELECT
        CASE
          WHEN AuthenticationResult LIKE '%Pass%' OR AuthenticationResult LIKE '%HumanDetect%' THEN 'Present'
          WHEN AuthenticationResult LIKE '%Fail%' THEN 'Absent'
          WHEN AuthenticationResult LIKE '%Invaild%' THEN 'Denied'
          ELSE 'Unknown'
        END as AttendanceStatus,
        COUNT(*) as count
      FROM AccessLogs ${baseCondition}
      GROUP BY
        CASE
          WHEN AuthenticationResult LIKE '%Pass%' OR AuthenticationResult LIKE '%HumanDetect%' THEN 'Present'
          WHEN AuthenticationResult LIKE '%Fail%' THEN 'Absent'
          WHEN AuthenticationResult LIKE '%Invaild%' THEN 'Denied'
          ELSE 'Unknown'
        END
    `, params);

    const dailyTrend = await dbQuery(`
      SELECT DATE(AccessDateTime) as date, COUNT(*) as count
      FROM AccessLogs ${baseCondition}
      GROUP BY DATE(AccessDateTime)
      ORDER BY date
    `, params);

    res.json({
      totalEntries: totalEntries[0]?.total || 0,
      todayEntries: todayEntries[0]?.today || 0,
      presentCount: presentCount[0]?.present || 0,
      deviceStats: deviceStats,
      statusStats: statusStats,
      dailyTrend: dailyTrend
    });
  } catch (err) {
    console.error('Error fetching attendance stats:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== REPORTS API ==========

// SHIFT PERFORMANCE REPORT
app.get('/api/reports/shift-performance', async (req, res) => {
  try {
    const { startDate = '2025-10-01', endDate = '2025-10-31', shiftType } = req.query;
    
    let whereClause = 'WHERE v.AccessDate BETWEEN ? AND ?';
    const params = [startDate, endDate];
    
    if (shiftType === 'regular') {
      whereClause += ' AND v.is_special_shift = 0';
    } else if (shiftType === 'special') {
      whereClause += ' AND v.is_special_shift = 1';
    }
    
    const query = `
      SELECT
        s.shift_id,
        s.shift_name,
        s.is_special_shift,
        COUNT(DISTINCT v.ParentID) as total_parents,
        COUNT(*) as total_shifts,
        COALESCE(SUM(v.money_earned_rwf), 0) as total_money,
        COALESCE(SUM(v.fee_earned_rwf), 0) as total_fees,
        COALESCE(SUM(v.money_earned_rwf + v.fee_earned_rwf), 0) as total_earnings,
        COALESCE(ROUND(AVG(v.money_earned_rwf + v.fee_earned_rwf), 0), 0) as avg_per_parent,
        COALESCE(ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(DISTINCT ParentID) FROM v_attendance_with_rates WHERE AccessDate BETWEEN ? AND ?), 0), 1), 0) as utilization_percentage
      FROM shifts s
      LEFT JOIN v_attendance_with_rates v ON s.shift_id = v.shift_id
        ${whereClause}
      GROUP BY s.shift_id, s.shift_name, s.is_special_shift
      ORDER BY s.start_time
    `;
    
    params.push(startDate, endDate);
    
    const results = await dbQuery(query, params);
    res.json(results);
  } catch (err) {
    console.error('Error generating shift report:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// SHIFT WORKED REPORT - NEW ENDPOINT FOR Reports.jsx
app.get('/api/reports/shift-worked', async (req, res) => {
  try {
    const { startDate = '2025-10-01', endDate = '2025-10-31' } = req.query;

    // Primary query using v_attendance_with_rates view (matches existing report patterns)
    let query = `
      SELECT 
        COALESCE(v.shift_name, 'Unknown') as shift_name,
        v.ParentID,
        COALESCE(p.FullName, 'Unknown') as FullName,
        v.check_in_time,
        v.check_out_time,
        v.device_type_for_calc,
        CASE 
          WHEN v.check_out_time IS NOT NULL AND v.check_in_time IS NOT NULL THEN true 
          ELSE false 
        END as valid
      FROM v_attendance_with_rates v
      LEFT JOIN Parents p ON v.ParentID = p.ParentID
      WHERE DATE(v.AccessDate) BETWEEN ? AND ?
      ORDER BY v.AccessDate DESC, v.check_in_time DESC
    `;

    let results;
    try {
      [results] = await db.execute(query, [startDate, endDate]);
    } catch (viewError) {
      console.warn('v_attendance_with_rates view unavailable, using AccessLogs fallback:', viewError.message);
      
      // Fallback query using AccessLogs + shifts (matches frontend expectations)
      query = `
        SELECT 
          COALESCE(s.shift_name, 'Morning Shift') as shift_name,
          al.ParentID,
          COALESCE(p.FullName, al.PersonName, 'Unknown') as FullName,
          TIME(al.AccessDateTime) as check_in_time,
          NULL as check_out_time,
          CASE
            WHEN al.DeviceName LIKE '%Money%' OR al.DeviceName = 'Gukorera Amafaranga' THEN 'money'
            WHEN al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' OR al.DeviceName = 'Gukorea Abana' THEN 'fees'
            ELSE 'other'
          END as device_type_for_calc,
          CASE WHEN al.AuthenticationResult LIKE '%Pass%' OR al.AuthenticationResult LIKE '%HumanDetect%' THEN true ELSE false END as valid
        FROM AccessLogs al
        LEFT JOIN Parents p ON al.ParentID = p.ParentID
        LEFT JOIN shifts s ON 1=1  -- Simplified shift matching
        WHERE DATE(al.AccessDateTime) BETWEEN ? AND ?
        ORDER BY al.AccessDateTime DESC
      `;
      [results] = await db.execute(query, [startDate, endDate]);
    }

    console.log(`📊 /api/reports/shift-worked: Returned ${results.length} records for ${startDate} to ${endDate}`);
    res.json(results);
  } catch (err) {
    console.error('Error in /api/reports/shift-worked:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// PARENT PERFORMANCE BY SHIFT
app.get('/api/reports/parent-performance', async (req, res) => {
  try {
    const { shiftId, startDate = '2025-10-01', endDate = '2025-10-31' } = req.query;
    
    if (!shiftId) {
      return res.status(400).json({ error: 'Shift ID is required' });
    }
    
    const query = `
      SELECT
        p.ParentID,
        p.FullName,
        COUNT(DISTINCT v.AccessDate) as days_worked,
        COUNT(*) as shifts_worked,
        COALESCE(SUM(v.money_earned_rwf), 0) as money_earned,
        COALESCE(SUM(v.fee_earned_rwf), 0) as fees_earned,
        COALESCE(SUM(v.money_earned_rwf + v.fee_earned_rwf), 0) as total_earnings,
        COALESCE(ROUND(AVG(v.money_earned_rwf + v.fee_earned_rwf), 0), 0) as avg_per_shift,
        COALESCE(ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(DISTINCT AccessDate) FROM v_attendance_with_rates WHERE shift_id = ? AND AccessDate BETWEEN ? AND ?), 0), 1), 0) as attendance_percentage,
        COALESCE(ROUND(SUM(TIME_TO_SEC(TIMEDIFF(v.check_out_time, v.check_in_time))) / 3600, 1), 0) as total_hours
      FROM Parents p
      JOIN v_attendance_with_rates v ON p.ParentID = v.ParentID
      WHERE v.shift_id = ? AND v.AccessDate BETWEEN ? AND ?
      GROUP BY p.ParentID, p.FullName
      ORDER BY total_earnings DESC
    `;
    
    const results = await dbQuery(query, [shiftId, startDate, endDate, shiftId, startDate, endDate]);
    res.json(results);
  } catch (err) {
    console.error('Error generating parent performance report:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== DASHBOARD STATISTICS ==========

app.get('/api/dashboard/stats', async (req, res) => {
  try {
    const latestDataDate = await dbGet(`
      SELECT MAX(AccessDate) as max_date
      FROM v_attendance_with_rates
    `);

    const referenceDate = latestDataDate?.max_date ? new Date(latestDataDate.max_date) : new Date();
    const refYear = referenceDate.getFullYear();
    const refMonth = referenceDate.getMonth() + 1;

    const monthStart = `${refYear}-${String(refMonth).padStart(2, '0')}-01`;
    const monthEndDate = new Date(refYear, refMonth, 0);
    const monthEnd = `${refYear}-${String(refMonth).padStart(2, '0')}-${String(monthEndDate.getDate()).padStart(2, '0')}`;
    
    const [
      totalParents,
      totalStudents,
      monthAttendance,
      activeShifts,
      monthEarnings,
      deviceStatus
    ] = await Promise.all([
      dbGet('SELECT COUNT(*) as count FROM Parents'),
      dbGet('SELECT COUNT(*) as count FROM Students'),
      dbGet(`
        SELECT COUNT(DISTINCT ParentID) as count
        FROM v_attendance_with_rates
        WHERE AccessDate BETWEEN ? AND ?
      `, [monthStart, monthEnd]),
      dbGet('SELECT COUNT(*) as count FROM shifts WHERE status = 1'),
      dbGet(`
        SELECT COALESCE(SUM(money_earned_rwf + fee_earned_rwf), 0) as total 
        FROM v_attendance_with_rates 
        WHERE AccessDate BETWEEN ? AND ?
      `, [monthStart, monthEnd]),
      dbGet('SELECT COUNT(*) as active FROM devices WHERE status = 1')
    ]);
    
    // Get monthly earnings trend (daily points within selected month)
    const monthlyTrend = await dbQuery(`
      SELECT 
        DATE(AccessDate) as date,
        COALESCE(SUM(money_earned_rwf + fee_earned_rwf), 0) as daily_earnings
      FROM v_attendance_with_rates 
      WHERE AccessDate BETWEEN ? AND ?
      GROUP BY DATE(AccessDate)
      ORDER BY date
    `, [monthStart, monthEnd]);
    
    // Get top earning parents for selected month
    const topEarnersMonth = await dbQuery(`
      SELECT 
        p.FullName,
        SUM(v.money_earned_rwf + v.fee_earned_rwf) as month_earnings
      FROM v_attendance_with_rates v
      JOIN Parents p ON v.ParentID = p.ParentID
      WHERE v.AccessDate BETWEEN ? AND ?
      GROUP BY p.ParentID, p.FullName
      ORDER BY month_earnings DESC
      LIMIT 5
    `, [monthStart, monthEnd]);
    
    res.json({
      totalParents: totalParents?.count || 0,
      totalStudents: totalStudents?.count || 0,
      todayAttendance: monthAttendance?.count || 0,
      activeShifts: activeShifts?.count || 0,
      todayEarnings: monthEarnings?.total || 0,
      activeDevices: deviceStatus?.active || 0,
      monthlyRange: {
        startDate: monthStart,
        endDate: monthEnd,
        year: refYear,
        month: refMonth
      },
      monthAttendance: monthAttendance?.count || 0,
      monthEarnings: monthEarnings?.total || 0,
      monthlyTrend: monthlyTrend,
      topEarnersMonth: topEarnersMonth,
      // Keep backward-compatible keys used by existing frontend cards/charts.
      weeklyTrend: monthlyTrend,
      topEarnersToday: topEarnersMonth
    });
  } catch (err) {
    console.error('Error fetching dashboard stats:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// Shift-specific dashboard card stats (DB-backed)
app.get('/api/dashboard/shift-card-stats', async (req, res) => {
  try {
    const now = new Date();
    const year = Number(req.query.year) || now.getFullYear();
    const month = Number(req.query.month) || (now.getMonth() + 1);

    const row = await dbGet(`
      SELECT
        (SELECT COUNT(*) FROM shifts WHERE status = 1) as active_shifts,
        (SELECT COUNT(*) FROM devices WHERE status = 1) as active_devices,
        (SELECT COUNT(*) FROM v_attendance_with_rates WHERE YEAR(AccessDate) = ? AND MONTH(AccessDate) = ?) as shifts_per_month
    `, [year, month]);

    res.json({
      year,
      month,
      activeShifts: Number(row?.active_shifts || 0),
      activeDevices: Number(row?.active_devices || 0),
      shiftsPerMonth: Number(row?.shifts_per_month || 0)
    });
  } catch (err) {
    console.error('Error fetching shift card stats:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== DEVICES MANAGEMENT ==========

// Get all devices
app.get('/api/devices/all', async (req, res) => {
  try {
    const devices = await dbQuery(`
      SELECT * FROM devices 
      ORDER BY DeviceName
    `);
    res.json(devices);
  } catch (err) {
    console.error('Error fetching all devices:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Add new device
app.post('/api/devices', async (req, res) => {
  try {
    const { DeviceName, device_type, ip_address, location, status = 1 } = req.body;
    
    const result = await dbRun(
      `INSERT INTO devices (DeviceName, device_type, ip_address, location, status, created_at) 
       VALUES (?, ?, ?, ?, ?, NOW())`,
      [DeviceName, device_type, ip_address, location, status]
    );
    
    res.json({ 
      message: 'Device added successfully', 
      id: result.lastID 
    });
  } catch (err) {
    console.error('Error adding device:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Update device
app.put('/api/devices/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { DeviceName, device_type, ip_address, location, status } = req.body;
    
    const result = await dbRun(
      `UPDATE devices SET 
        DeviceName = ?, device_type = ?, ip_address = ?, location = ?, status = ?, updated_at = NOW()
       WHERE device_id = ?`,
      [DeviceName, device_type, ip_address, location, status, id]
    );
    
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Device not found' });
    }
    
    res.json({ message: 'Device updated successfully' });
  } catch (err) {
    console.error('Error updating device:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Delete device
app.delete('/api/devices/:id', async (req, res) => {
  try {
    const { id } = req.params;
    
    const result = await dbRun(
      'DELETE FROM devices WHERE device_id = ?',
      [id]
    );
    
    if (result.changes === 0) {
      return res.status(404).json({ error: 'Device not found' });
    }
    
    res.json({ message: 'Device deleted successfully' });
  } catch (err) {
    console.error('Error deleting device:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// ========== SEARCH ENDPOINTS ==========

// Search parents
app.get('/api/search/parents', async (req, res) => {
  try {
    const { query } = req.query;
    
    if (!query || query.trim() === '') {
      return res.json([]);
    }
    
    const results = await dbQuery(`
      SELECT ParentID, FullName, PhoneNumber, Gender, NumberOfKids
      FROM Parents
      WHERE FullName LIKE ? 
         OR PhoneNumber LIKE ?
      ORDER BY FullName
      LIMIT 20
    `, [`%${query}%`, `%${query}%`]);
    
    res.json(results);
  } catch (err) {
    console.error('Error searching parents:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// Search students
app.get('/api/search/students', async (req, res) => {
  try {
    const { query } = req.query;
    
    if (!query || query.trim() === '') {
      return res.json([]);
    }
    
    const results = await dbQuery(`
      SELECT StudentID, Registration_Number, FirstName, LastName, Class
      FROM Students
      WHERE FirstName LIKE ? 
         OR LastName LIKE ?
         OR Registration_Number LIKE ?
      ORDER BY LastName, FirstName
      LIMIT 20
    `, [`%${query}%`, `%${query}%`, `%${query}%`]);
    
    res.json(results);
  } catch (err) {
    console.error('Error searching students:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

// ========== MONEY SHIFTS ENDPOINTS ==========

// Get money shifts data - UPDATED to use AccessLogs table directly
app.get('/api/money-shifts', async (req, res) => {
  try {
    const { 
      startDate = '2025-10-01', 
      endDate = '2025-10-31', 
      shiftId,
      parentId,
      deviceType,
      search = ''
    } = req.query;

    let whereClause = "WHERE DATE(al.AccessDateTime) BETWEEN ? AND ?";
    const params = [startDate, endDate];
    const effectiveSearch = String(search || '').trim();

    // Filter by device type (money or fees)
    if (deviceType === 'money') {
      whereClause += " AND (DeviceName = 'Gukorera Amafaranga' OR DeviceName LIKE '%Money%')";
    } else if (deviceType === 'fees') {
      whereClause += " AND (DeviceName = 'Gukorea Abana' OR DeviceName LIKE '%Fee%' OR DeviceName LIKE '%Abana%')";
    }

    if (shiftId) {
      whereClause += ' AND EXISTS (SELECT 1 FROM shifts WHERE shift_id = ?)';
      params.push(shiftId);
    }

    if (parentId) {
      whereClause += ' AND al.ParentID = ?';
      params.push(parentId);
    }

    if (effectiveSearch) {
      whereClause += ' AND (CAST(al.ParentID AS CHAR) LIKE ? OR p.FullName LIKE ? OR al.PersonName LIKE ?)';
      const term = `%${effectiveSearch}%`;
      params.push(term, term, term);
    }

      
    // Get detailed shift data from AccessLogs with parent information
    const query = `
      SELECT
        al.LogID,
        al.ParentID,
        al.AccessDateTime,
        DATE(al.AccessDateTime) as AccessDate,
        TIME(al.AccessDateTime) as AccessTime,
        al.AuthenticationResult,
        al.DeviceName,
        al.DeviceSerialNo,
        al.PersonName,
        al.PersonGroup,
        al.CardNumber,
        COALESCE(al.PersonName, p.FullName, 'Unknown') as ParentName,
        p.FullName as ParentFullName,
        p.PhoneNumber as ParentPhone,
        p.Gender as ParentGender,
        p.NumberOfKids as ParentKids,
        CASE
          WHEN al.AuthenticationResult LIKE '%Pass%' OR al.AuthenticationResult LIKE '%HumanDetect%' THEN 'Present'
          WHEN al.AuthenticationResult LIKE '%Fail%' THEN 'Absent'
          WHEN al.AuthenticationResult LIKE '%Invaild%' THEN 'Denied'
          ELSE 'Unknown'
        END as AttendanceStatus,
        CASE
          WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 'money'
          WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 'fees'
          ELSE 'other'
        END as device_type
      FROM AccessLogs al
      LEFT JOIN Parents p ON al.ParentID = p.ParentID
      ${whereClause}
      ORDER BY al.AccessDateTime DESC
      LIMIT 1000
    `;

    const results = await dbQuery(query, params);

    // Get summary stats
    const summaryQuery = `
      SELECT
        COUNT(DISTINCT al.ParentID) as total_parents,
        COUNT(*) as total_events,
        COUNT(DISTINCT DATE(al.AccessDateTime)) as total_days,
        SUM(CASE WHEN al.AuthenticationResult LIKE '%Pass%' OR al.AuthenticationResult LIKE '%HumanDetect%' THEN 1 ELSE 0 END) as present_count,
        SUM(CASE WHEN al.AuthenticationResult LIKE '%Fail%' THEN 1 ELSE 0 END) as absent_count,
        SUM(CASE WHEN al.AuthenticationResult LIKE '%Invaild%' THEN 1 ELSE 0 END) as denied_count
      FROM AccessLogs al
      LEFT JOIN Parents p ON al.ParentID = p.ParentID
      ${whereClause}
    `;

    const summary = await dbQuery(summaryQuery, params);

    // Group events by parent and date for easier analysis
    const groupedByParentDate = {};
    results.forEach(record => {
      const key = `${record.ParentID}-${record.AccessDate}`;
      if (!groupedByParentDate[key]) {
        groupedByParentDate[key] = {
          ParentID: record.ParentID,
          AccessDate: record.AccessDate,
          firstEntry: record.AccessTime,
          lastEntry: record.AccessTime,
          entries: [],
          deviceType: record.device_type
        };
      }
      groupedByParentDate[key].entries.push({
        time: record.AccessTime,
        status: record.AttendanceStatus,
        device: record.DeviceName
      });
      
      // Update first and last entry times
      if (record.AccessTime < groupedByParentDate[key].firstEntry) {
        groupedByParentDate[key].firstEntry = record.AccessTime;
      }
      if (record.AccessTime > groupedByParentDate[key].lastEntry) {
        groupedByParentDate[key].lastEntry = record.AccessTime;
      }
    });

    // Convert grouped data to array
    const groupedArray = Object.values(groupedByParentDate);

    res.json({
      rawShifts: results,
      groupedShifts: groupedArray,
      summary: summary[0] || {
        total_parents: 0,
        total_events: 0,
        total_days: 0,
        present_count: 0,
        absent_count: 0,
        denied_count: 0
      }
    });
  } catch (err) {
    console.error('Error fetching money shifts:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// Get money shift earnings calculation - FIXED for Shifts.jsx/MoneyShifts.jsx
app.get('/api/money-shifts/earnings', async (req, res) => {
  try {
    const { 
      startDate = new Date().getFullYear() + '-' + String(new Date().getMonth() + 1).padStart(2, '0') + '-01',
      endDate = new Date().getFullYear() + '-' + String(new Date().getMonth() + 1).padStart(2, '0') + '-' + new Date(new Date().getFullYear(), new Date().getMonth() + 1, 0).getDate(),
      search = ''
    } = req.query;

    console.log(`📊 Money shifts earnings query: ${startDate} to ${endDate}, search: "${search}"`);

    const effectiveSearch = String(search || '').trim();


    // Use AttendanceLog table for shift data
    let whereClause = 'WHERE DATE(al.AccessDateTime) BETWEEN ? AND ?';
    const params = [startDate, endDate];

    if (effectiveSearch) {
      whereClause += ' AND (CAST(al.ParentID AS CHAR) LIKE ? OR p.FullName LIKE ? OR al.PersonName LIKE ?)';
      params.push(`%${effectiveSearch}%`, `%${effectiveSearch}%`, `%${effectiveSearch}%`);
    }

    const recordsQuery = `
      SELECT
        al.ParentID,
        COALESCE(p.FullName, al.PersonName, 'Unknown') as FullName,
        DATE(al.AccessDateTime) as AccessDate,
        'Money Shift' as shift_name,  -- Can enhance with shifts table join if needed
        TIME(al.AccessDateTime) as check_in_time,
        TIME(al.AccessDateTime) as check_out_time,  -- Simplified; real check-out needs shift matching
        CASE
          WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 'money'
          WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 'fees'
          ELSE 'other'
        END as device_type_for_calc,
        CASE
          WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 2500
          ELSE 0
        END as money_earned_rwf,
        CASE
          WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 2000
          ELSE 0
        END as fee_earned_rwf,
        CASE
          WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 2500
          WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 2000
          ELSE 0
        END as total_earned,
        CASE
          WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 2500
          WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 2000
          ELSE 2500  -- Default money rate
        END as applied_rate,
        al.DeviceName,
        al.AuthenticationResult
      FROM AttendanceLog al
      LEFT JOIN Parents p ON al.ParentID = p.ParentID
      ${whereClause}
      HAVING total_earned > 0  -- Only shifts with earnings
      ORDER BY AccessDate DESC, check_in_time
    `;

    // Execute the query and get detailedEarnings
    const [detailedEarnings] = await db.query(recordsQuery, params);

    // Mark valid shifts (e.g., duplicate check-in/out for same parent, date, and device_type_for_calc)
    const validShiftMap = {};
    detailedEarnings.forEach(row => {
      const key = `${row.ParentID}|${row.AccessDate}|${row.device_type_for_calc}`;
      if (!validShiftMap[key]) validShiftMap[key] = [];
      validShiftMap[key].push(row);
    });

    // Mark valid: true if duplicate, false if unique
    const detailedEarningsWithValid = detailedEarnings.map(row => {
      const key = `${row.ParentID}|${row.AccessDate}|${row.device_type_for_calc}`;
      return {
        ...row,
        valid: validShiftMap[key].length > 1
      };
    });

    // Only calculate earnings for non-valid shifts
    const totals = detailedEarningsWithValid.reduce((acc, row) => {
      if (!row.valid) {
        acc.totalMoney += Number(row.money_earned_rwf || 0);
        acc.totalFees += Number(row.fee_earned_rwf || 0);
        acc.totalEarnings += Number(row.total_earned || 0);
      }
      return acc;
    }, { totalMoney: 0, totalFees: 0, totalEarnings: 0 });

    // Group by parent for summary (matching MoneyShifts.jsx)
    const parentSummary = {};
    detailedEarningsWithValid.forEach(row => {
      const pid = row.ParentID;
      if (!parentSummary[pid]) {
        parentSummary[pid] = {
          ParentID: pid,
          FullName: row.FullName,
          totalMoney: 0,
          totalFees: 0,
          totalEarnings: 0,
          shiftsCount: 0,
          validShifts: 0,
          daysWorked: new Set()
        };
      }
      if (!row.valid) {
        parentSummary[pid].totalMoney += Number(row.money_earned_rwf || 0);
        parentSummary[pid].totalFees += Number(row.fee_earned_rwf || 0);
        parentSummary[pid].totalEarnings += Number(row.total_earned || 0);
        parentSummary[pid].shiftsCount += 1;
        parentSummary[pid].daysWorked.add(row.AccessDate);
      } else {
        parentSummary[pid].validShifts += 1;
      }
    });

    const parentSummaryArray = Object.values(parentSummary).map(ps => ({
      ...ps,
      daysWorked: ps.daysWorked.size
    }));

    const response = {
      detailedEarnings: detailedEarningsWithValid,
      parentSummary: parentSummaryArray,
      totals
    };

    console.log(`✅ Earnings response: ${detailedEarningsWithValid.length} records, ${parentSummaryArray.length} parents, totals: ${JSON.stringify(totals)}`);

    res.json(response);

  } catch (err) {
    console.error('❌ Error in /api/money-shifts/earnings:', err);
    res.status(500).json({ error: 'Server error: ' + err.message });
  }
});


// ========== FEES MANAGEMENT ENDPOINTS ==========

// Get all fees
app.get('/api/fees', async (req, res) => {
  try {
    // Query the student_fee_payments table which is created at startup
    const fees = await dbQuery('SELECT * FROM student_fee_payments ORDER BY payment_date DESC');
    res.json(fees);
  } catch (err) {
    console.error('Error fetching fees:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// Add new fee payment
app.post('/api/fees', async (req, res) => {
  try {
    const { StudentID, Amount, PaymentDate, PaymentMethod, Notes } = req.body;

    if (!StudentID || !Amount || !PaymentDate || !PaymentMethod) {
      return res.status(400).json({ error: 'StudentID, Amount, PaymentDate, and PaymentMethod are required' });
    }

    const result = await dbRun(
      'INSERT INTO student_fee_payments (student_id, amount, payment_date, payment_method, notes) VALUES (?, ?, ?, ?, ?)',
      [StudentID, Amount, PaymentDate, PaymentMethod, Notes || null]
    );

    res.json({
      id: result.lastID,
      student_id: StudentID,
      amount: Amount,
      payment_date: PaymentDate,
      payment_method: PaymentMethod,
      notes: Notes,
      created_at: new Date().toISOString()
    });
  } catch (err) {
    console.error('Error adding fee:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// Import student classes from Excel/CSV
app.post('/api/fees/import-classes', async (req, res) => {
  try {
    const { data } = req.body;
    
    if (!data || !Array.isArray(data)) {
      return res.status(400).json({ error: 'Invalid data format. Expected array of student records.' });
    }

    let updated = 0;
    let failed = 0;
    const errors = [];

    for (const record of data) {
      try {
        const studentId = record.StudentID || record.student_id;
        const newClass = record.Class || record.class;

        if (!studentId || !newClass) {
          failed++;
          errors.push(`Missing studentId or class for record: ${JSON.stringify(record)}`);
          continue;
        }

        const result = await dbRun(
          'UPDATE Students SET Class = ? WHERE StudentID = ?',
          [newClass, studentId]
        );

        if (result.changes > 0) {
          updated++;
        } else {
          failed++;
          errors.push(`Student not found with ID: ${studentId}`);
        }
      } catch (recordError) {
        failed++;
        errors.push(`Error processing record: ${recordError.message}`);
      }
    }

    res.json({
      message: 'Import completed',
      updated: updated,
      failed: failed,
      total: data.length,
      errors: errors.slice(0, 10)
    });
  } catch (err) {
    console.error('Error importing classes:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== IMPORT HISTORY ENDPOINTS ==========

// Get import history
app.get('/api/import-history', async (req, res) => {
  try {
    // Check if import_history table exists, if not return empty array
    let history = [];
    try {
      history = await dbQuery('SELECT * FROM import_history ORDER BY importDate DESC');
    } catch (err) {
      console.log('Import history table does not exist, returning empty array');
      // You can uncomment the following lines to create the import_history table automatically
      /*
      await dbQuery(`
        CREATE TABLE IF NOT EXISTS import_history (
          id INT AUTO_INCREMENT PRIMARY KEY,
          fileName VARCHAR(255),
          totalRecords INT,
          successfulUpdates INT,
          failedUpdates INT,
          importDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          status VARCHAR(50),
          notes TEXT
        )
      `);
      */
    }
    res.json(history);
  } catch (err) {
    console.error('Error fetching import history:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// Add import history record
app.post('/api/import-history', async (req, res) => {
  try {
    const { fileName, totalRecords, successfulUpdates, failedUpdates, status, notes } = req.body;

    // Check if import_history table exists
    try {
      await dbQuery('SELECT 1 FROM import_history LIMIT 1');
    } catch (err) {
      // Create import_history table if it doesn't exist
      await dbQuery(`
        CREATE TABLE import_history (
          id INT AUTO_INCREMENT PRIMARY KEY,
          fileName VARCHAR(255),
          totalRecords INT,
          successfulUpdates INT,
          failedUpdates INT,
          importDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          status VARCHAR(50),
          notes TEXT
        )
      `);
    }

    const result = await dbRun(
      'INSERT INTO import_history (fileName, totalRecords, successfulUpdates, failedUpdates, status, notes) VALUES (?, ?, ?, ?, ?, ?)',
      [fileName, totalRecords, successfulUpdates, failedUpdates, status, notes || null]
    );

    res.json({
      id: result.lastID,
      fileName,
      totalRecords,
      successfulUpdates,
      failedUpdates,
      importDate: new Date().toISOString(),
      status,
      notes
    });
  } catch (err) {
    console.error('Error adding import history:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== STUDENT ALLOCATIONS API ==========

app.get('/api/student-allocations', async (req, res) => {
  try {
    // For now, return empty array since allocations table doesn't exist
    // You can implement this later if needed
    res.json([]);
  } catch (err) {
    console.error('Error fetching student allocations:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== SYSTEM STATUS ENDPOINT ==========

app.get('/api/status', async (req, res) => {
  try {
    const dbStatus = await dbGet('SELECT 1 as connected');
    const tableCounts = await dbQuery(`
      SELECT
        (SELECT COUNT(*) FROM Parents) as parents,
        (SELECT COUNT(*) FROM Students) as students,
        (SELECT COUNT(*) FROM AccessLogs) as access_logs,
        (SELECT COUNT(*) FROM shifts) as shifts,
        (SELECT COUNT(*) FROM devices) as devices,
        (SELECT COUNT(*) FROM parent_rates) as custom_rates,
        (SELECT COUNT(*) FROM StudentParent) as relations
    `);

    // Get today's activity
    const today = new Date().toISOString().split('T')[0];
    const todayActivity = await dbQuery(`
      SELECT
        COUNT(*) as today_logs,
        COUNT(DISTINCT ParentID) as today_parents
      FROM AccessLogs
      WHERE DATE(AccessDateTime) = ?
    `, [today]);

    res.json({
      database: 'connected',
      timestamp: new Date().toISOString(),
      tables: tableCounts[0],
      todayActivity: todayActivity[0] || { today_logs: 0, today_parents: 0 }
    });
  } catch (err) {
    console.error('Error getting status:', err);
    res.status(500).json({
      database: 'disconnected',
      error: err.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Get available devices
app.get('/api/devices', async (req, res) => {
  try {
    const devices = await dbQuery(`
      SELECT DISTINCT DeviceName, device_type 
      FROM devices 
      WHERE status = 1 
      ORDER BY DeviceName
    `);
    res.json(devices);
  } catch (err) {
    console.error('Error fetching devices:', err);
    // Fallback to AccessLogs if devices table doesn't exist
    try {
      const devices = await dbQuery(`
        SELECT DISTINCT DeviceName 
        FROM AccessLogs 
        WHERE DeviceName IS NOT NULL 
        ORDER BY DeviceName
      `);
      res.json(devices.map(d => ({ DeviceName: d.DeviceName, device_type: 'unknown' })));
    } catch (err2) {
      res.status(500).json({ error: 'Database error' });
    }
  }
});

// ========== SERVE STATIC FILES ==========

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'login.html'));
});

app.get('/index.html', (req, res) => {
  res.sendFile(path.join(__dirname, 'login.html'));
});

app.get('/parents', (req, res) => {
  res.sendFile(path.join(__dirname, 'parents.html'));
});

app.get('/students', (req, res) => {
  res.sendFile(path.join(__dirname, 'students.html'));
});

app.get('/relations', (req, res) => {
  res.sendFile(path.join(__dirname, 'relations.html'));
});

app.get('/attendance', (req, res) => {
  res.sendFile(path.join(__dirname, 'attendance.html'));
});

app.get('/earnings', (req, res) => {
  res.sendFile(path.join(__dirname, 'earnings.html'));
});

app.get('/dashboard', (req, res) => {
  res.sendFile(path.join(__dirname, 'dashboard.html'));
});

app.get('/shifts', (req, res) => {
  res.sendFile(path.join(__dirname, 'Moneyshift.html'));
});

app.get('/settings', (req, res) => {
  res.sendFile(path.join(__dirname, 'settings.html'));
});

app.get('/shift-manager.html', (req, res) => {
  res.sendFile(path.join(__dirname, 'shift-manager.html'));
});

app.get('/Moneyshift.html', (req, res) => {
  res.sendFile(path.join(__dirname, 'Moneyshift.html'));
});

app.get('/login.html', (req, res) => {
  res.sendFile(path.join(__dirname, 'login.html'));
});

app.get('/school-fees.html', (req, res) => {
  res.sendFile(path.join(__dirname, 'school-fees.html'));
});

// ========== SCHOOL FEES MANAGEMENT ENDPOINTS ==========

// Get students with fees information
app.get('/api/school-fees/students', async (req, res) => {
  try {
    const { search, classFilter, minAmount, maxAmount } = req.query;

    let whereClause = 'WHERE 1=1';
    const params = [];

    if (search) {
      whereClause += ' AND (s.FirstName LIKE ? OR s.LastName LIKE ? OR s.Registration_Number LIKE ?)';
      params.push(`%${search}%`, `%${search}%`, `%${search}%`);
    }

    if (classFilter) {
      whereClause += ' AND s.Class = ?';
      params.push(classFilter);
    }

    // Get students with their fee information
    const query = `
      SELECT
        s.StudentID,
        s.Registration_Number,
        s.FirstName,
        s.LastName,
        s.Class,
        COALESCE(pa.parent_name, 'N/A') as parent_name,
        COALESCE(pa.Relationship, 'N/A') as Relationship,
        COALESCE(fa.total_fees, 0) as total_fees,
        COALESCE(fa.total_paid, 0) as total_paid,
        COALESCE(fa.total_balance, 0) as total_balance
      FROM Students s
      LEFT JOIN (
        SELECT
          sp.StudentID,
          GROUP_CONCAT(DISTINCT p.FullName ORDER BY p.FullName SEPARATOR ', ') as parent_name,
          GROUP_CONCAT(DISTINCT sp.Relationship ORDER BY sp.Relationship SEPARATOR ', ') as Relationship
        FROM StudentParent sp
        LEFT JOIN Parents p ON sp.ParentID = p.ParentID
        GROUP BY sp.StudentID
      ) pa ON pa.StudentID = s.StudentID
      LEFT JOIN (
        SELECT
          f.student_id,
          COALESCE(SUM(f.amount), 0) as total_fees,
          COALESCE(SUM(CASE WHEN COALESCE(f.amount_paid, 0) > 0 THEN f.amount_paid ELSE f.amount END), 0) as total_paid,
          COALESCE(SUM(
            GREATEST(
              COALESCE(f.amount, 0) - (CASE WHEN COALESCE(f.amount_paid, 0) > 0 THEN f.amount_paid ELSE f.amount END),
              0
            )
          ), 0) as total_balance
        FROM student_fee_payments f
        GROUP BY f.student_id
      ) fa ON fa.student_id = s.StudentID
      ${whereClause}
      ORDER BY s.LastName, s.FirstName
    `;

    const students = await dbQuery(query, params);

    // Get filters data
    const totalStudents = await dbGet('SELECT COUNT(*) as count FROM Students');
    const totalFees = await dbGet('SELECT COALESCE(SUM(amount), 0) as total FROM student_fee_payments');
    const totalPaid = await dbGet(
      'SELECT COALESCE(SUM(CASE WHEN COALESCE(amount_paid, 0) > 0 THEN amount_paid ELSE amount END), 0) as total FROM student_fee_payments'
    );
    const totalBalance = await dbGet(
      `SELECT COALESCE(SUM(
        GREATEST(
          COALESCE(amount, 0) - (CASE WHEN COALESCE(amount_paid, 0) > 0 THEN amount_paid ELSE amount END),
          0
        )
      ), 0) as total FROM student_fee_payments`
    );

    // Get unique classes
    const classes = await dbQuery('SELECT DISTINCT Class FROM Students WHERE Class IS NOT NULL ORDER BY Class');

    res.json({
      students: students,
      filters: {
        totalStudents: totalStudents?.count || 0,
        totalFees: totalFees?.total || 0,
        totalPaid: totalPaid?.total || 0,
        totalBalance: totalBalance?.total || 0,
        classes: classes.map(c => c.Class)
      }
    });
  } catch (err) {
    console.error('Error fetching students with fees:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// Get student fee details
app.get('/api/school-fees/student/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { startDate, endDate } = req.query;

    // Get student info
    const student = await dbGet('SELECT * FROM Students WHERE StudentID = ?', [id]);
    if (!student) {
      return res.status(404).json({ error: 'Student not found' });
    }

    // Get parent info
    const parentQuery = `
      SELECT p.*, sp.Relationship
      FROM StudentParent sp
      JOIN Parents p ON sp.ParentID = p.ParentID
      WHERE sp.StudentID = ?
      LIMIT 1
    `;
    const parent = await dbGet(parentQuery, [id]);

    // Get fee payments
    let feeQuery = `
      SELECT
        f.*, 
        (CASE WHEN COALESCE(f.amount_paid, 0) > 0 THEN f.amount_paid ELSE f.amount END) as effective_paid,
        GREATEST(
          COALESCE(f.amount, 0) - (CASE WHEN COALESCE(f.amount_paid, 0) > 0 THEN f.amount_paid ELSE f.amount END),
          0
        ) as remaining_balance
      FROM student_fee_payments f
      WHERE f.student_id = ?
    `;
    const params = [id];

    if (startDate && endDate) {
      feeQuery += ' AND payment_date BETWEEN ? AND ?';
      params.push(startDate, endDate);
    }

    feeQuery += ' ORDER BY payment_date DESC';
    const fees = await dbQuery(feeQuery, params);

    // Calculate totals
    const totals = fees.reduce((acc, fee) => {
      const amount = Number(fee.amount) || 0;
      const paid = Number(fee.effective_paid) || 0;
      const balance = Number(fee.remaining_balance) || 0;
      acc.total_fees += amount;
      acc.total_paid += paid;
      acc.total_balance += balance;
      return acc;
    }, { total_fees: 0, total_paid: 0, total_balance: 0 });

    totals.payments_count = fees.length;

    res.json({
      student: student,
      parent: parent,
      fees: fees,
      totals: totals
    });
  } catch (err) {
    console.error('Error fetching student fee details:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// Add fee payment
app.post('/api/school-fees/payment', async (req, res) => {
  try {
    const {
      student_id,
      student_name,
      class: studentClass,
      term,
      year,
      amount,
      amount_paid,
      payment_date,
      payment_method
    } = req.body;

    if (!student_id || !amount || !payment_date) {
      return res.status(400).json({ error: 'Student ID, amount, and payment date are required' });
    }

    // Check if student exists
    const student = await dbGet('SELECT * FROM Students WHERE StudentID = ?', [student_id]);
    if (!student) {
      return res.status(404).json({ error: 'Student not found' });
    }

    const result = await dbRun(
      `INSERT INTO student_fee_payments (
        student_id, amount, payment_date, term, year, payment_method, amount_paid, notes
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        student_id,
        amount,
        payment_date,
        term || null,
        year || new Date().getFullYear(),
        payment_method || 'Cash',
        amount_paid || 0,
        `Payment for ${student_name} (${studentClass})`
      ]
    );

    res.json({
      id: result.lastID,
      student_id,
      amount,
      payment_date,
      term,
      year,
      payment_method,
      amount_paid,
      message: 'Fee payment recorded successfully'
    });
  } catch (err) {
    console.error('Error adding fee payment:', err);
    res.status(500).json({ error: 'Database error: ' + err.message });
  }
});

// ========== ERROR HANDLING ==========

app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

app.use((req, res) => {
  res.status(404).json({ error: 'Endpoint not found' });
});

// ========== START SERVER ==========

const startServer = async () => {
  try {
    await initDB();
    await createFeesTable();
    await checkTables();
    
    // Check view existence
    try {
      const viewCheck = await dbQuery("SELECT COUNT(*) as count FROM v_attendance_with_rates LIMIT 1");
      console.log('✅ v_attendance_with_rates view is accessible');
    } catch (err) {
      console.warn('⚠️  v_attendance_with_rates view may not exist or have different structure');
    }
    
    // Check v_shift_events view
    try {
      const shiftEventsCheck = await dbQuery("SELECT COUNT(*) as count FROM v_shift_events LIMIT 1");
      console.log('✅ v_shift_events view is accessible');
    } catch (err) {
      console.warn('⚠️  v_shift_events view may not exist');
    }
    
    app.listen(PORT, () => {
      console.log(`🚀 Server running on http://localhost:${PORT}`);
      console.log(`📊 API available at http://localhost:${PORT}/api/`);
      console.log(`🔐 Login: http://localhost:${PORT}/login.html`);
      console.log(`📊 Database: ${dbConfig.database}@${dbConfig.host}`);
      console.log(`💰 Money Shifts: Updated to show real data from AccessLogs`);
      console.log(`📈 Earnings: Using v_attendance_with_rates view for calculations`);
      console.log(`✅ Features: Money shifts with real data, Earnings calculations, Dashboard`);
    });
  } catch (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
};

startServer();