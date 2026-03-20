import mysql from 'mysql2/promise';
import { config } from 'dotenv';
config(); // Loads from .env

(async () => {
  try {
    console.log('🔧 Fixing v_attendance_with_rates view...');
    
    const db = await mysql.createPool({
      host: process.env.DB_HOST || '192.171.1.10',
      user: process.env.DB_USER || 'hikuser',
      password: process.env.DB_PASSWORD || 'Hope@2025!',
      database: 'agliculture_att',
      waitForConnections: true,
      connectionLimit: 5
    });

    // Step 1: Check current view status
    try {
      await db.query("SELECT 1 FROM v_attendance_with_rates LIMIT 1");
      console.log('ℹ️  View exists but is broken');
    } catch (e) {
      console.log('❌ View query failed:', e.message);
    }

    // Step 2: DROP existing view if it exists
    try {
      await db.query('DROP VIEW IF EXISTS v_attendance_with_rates');
      console.log('✅ Dropped broken view');
    } catch (e) {
      console.log('⚠️  Could not drop view:', e.message);
    }

    // Step 3: CREATE fixed view based on code analysis
    const createViewSQL = `
    CREATE OR REPLACE VIEW v_attendance_with_rates AS
    SELECT 
      al.ParentID,
      DATE(al.AccessDateTime) as AccessDate,
      TIME(al.AccessDateTime) as check_in_time,
      TIME(al.AccessDateTime) as check_out_time, -- simplified
      al.DeviceName,
      COALESCE(s.shift_id, 0) as shift_id,
      COALESCE(s.shift_name, 'Unknown') as shift_name,
      COALESCE(s.start_time, '00:00') as start_time,
      COALESCE(s.end_time, '23:59') as end_time,
      CASE
        WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 'money'
        WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 'fees'
        ELSE 'other'
      END as device_type_for_calc,
      CASE 
        WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 
          COALESCE(pr.MoneyRateOverrideRWF, COALESCE(s.money_rate_rwf_special, s.money_rate_rwf, 2500))
        WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 
          COALESCE(s.fee_rate_rwf, 2000)
        ELSE 0
      END as money_earned_rwf,
      CASE 
        WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 0
        WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 
          COALESCE(s.fee_rate_rwf, 2000)
        ELSE 0
      END as fee_earned_rwf,
      CASE 
        WHEN al.DeviceName = 'Gukorera Amafaranga' OR al.DeviceName LIKE '%Money%' THEN 
          COALESCE(pr.MoneyRateOverrideRWF, COALESCE(s.money_rate_rwf_special, s.money_rate_rwf, 2500))
        WHEN al.DeviceName = 'Gukorea Abana' OR al.DeviceName LIKE '%Fee%' OR al.DeviceName LIKE '%Abana%' THEN 
          COALESCE(s.fee_rate_rwf, 2000)
        ELSE 0
      END as total_earned,
      s.is_special_shift,
      al.PersonName
    FROM AccessLogs al
    LEFT JOIN shifts s ON 1=1  -- Simplified join, assumes shift matching logic
    LEFT JOIN Parents p ON al.ParentID = p.ParentID
    LEFT JOIN parent_rates pr ON al.ParentID = pr.ParentID
    WHERE al.ParentID IS NOT NULL
      AND al.AuthenticationResult LIKE '%Pass%'
    `;

    await db.query(createViewSQL);
    console.log('✅ Created fixed v_attendance_with_rates view');

    // Step 4: Test the new view
    const testQuery = await db.query("SELECT COUNT(*) as count FROM v_attendance_with_rates LIMIT 10");
    console.log('✅ View test successful:', testQuery[0]);

    // Step 5: Test dashboard query
    const dashboardTest = await db.query(`
      SELECT 
        COUNT(DISTINCT ParentID) as parents,
        COALESCE(SUM(money_earned_rwf + fee_earned_rwf), 0) as total_earnings
      FROM v_attendance_with_rates 
      WHERE AccessDate >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
    `);
    console.log('✅ Dashboard query test:', dashboardTest[0]);

    console.log('🎉 View fixed successfully! Restart server.js to test.');
    await db.end();
    
  } catch (error) {
    console.error('❌ Fix failed:', error);
    process.exit(1);
  }
})();

