CREATE OR REPLACE PACKAGE collection_pkg
AS
  PROCEDURE get_emp_details(p_deptno IN Dept.Deptno%TYPE,
                            p_result OUT SYS_REFCURSOR
                           );

  PROCEDURE fetch_emp_rec(p_deptno IN Emp.Deptno%TYPE);
  
  PROCEDURE search_emp_details(p_empno  IN Emp.Empno%TYPE,
                               p_sal    IN Emp.Sal%TYPE,
                               p_deptno IN Dept.Deptno%TYPE,
                               p_result OUT SYS_REFCURSOR
                              );
END collection_pkg;
/
CREATE OR REPLACE PACKAGE BODY collection_pkg
AS
  PROCEDURE get_emp_details(p_deptno IN Dept.Deptno%TYPE,
                            p_result OUT SYS_REFCURSOR
                           )
  AS
    v_sql VARCHAR2(4000);

  BEGIN 

    v_sql := 'SELECT E.Empno AS Employee_ID,
                    E.Ename As Employee_Name,
                    E.Job   AS Job,
                    E.Sal   AS Salary,
                    E.Comm  AS Commission,
                    E.Sal + NVL(E.Comm, 0) AS Total_Salary,
                    (E.Sal + NVL(E.Comm, 0)) * 12 AS Salary_per_Anum,
                    D.Dname AS Department_Name
              FROM Emp E
              LEFT JOIN Dept D ON (E.Deptno = D.Deptno)
              WHERE E.Deptno = :deptno';

     OPEN p_result FOR v_sql USING p_deptno;

  END get_emp_details;
--=====================================================================================
  PROCEDURE fetch_emp_rec(p_deptno IN Emp.Deptno%TYPE)
  AS
    TYPE Emp_Record IS RECORD
    (Empno Emp.Empno%TYPE,
     Ename Emp.Ename%TYPE,
     Job   Emp.Job%TYPE,
     Sal   Emp.Sal%TYPE,
     Comm  Emp.Comm%TYPE,
     Tot_Sal NUMBER(10, 2),
     PA      NUMBER(10, 2),
     Dname Dept.Dname%TYPE
    );

    TYPE Emp_Typ IS TABLE OF Emp_Record;
    Emp_Obj Emp_Typ;
    
    v_result SYS_REFCURSOR;
  BEGIN
    get_emp_details(p_deptno => p_deptno, p_result => v_result);

    FETCH v_result BULK COLLECT INTO Emp_Obj;

    FORALL i IN Emp_Obj.FIRST.. Emp_Obj.LAST

      INSERT INTO Emp_Details(EMPNO,
                              ENAME,
                              JOB,
                              SAL,
                              COMM,
                              TOT_SAL,
                              PA,
                              DNAME
                             )
                    VALUES(Emp_Obj(i).EMPNO,
                           Emp_Obj(i).ENAME,
                           Emp_Obj(i).JOB,
                           Emp_Obj(i).SAL,
                           Emp_Obj(i).COMM,
                           Emp_Obj(i).TOT_SAL,
                           Emp_Obj(i).PA,
                           Emp_Obj(i).DNAME
                          );
    COMMIT;
  END fetch_emp_rec;
--=====================================================================================
  PROCEDURE search_emp_details(p_empno  IN Emp.Empno%TYPE,
                               p_sal    IN Emp.Sal%TYPE,
                               p_deptno IN Dept.Deptno%TYPE,
                               p_result OUT SYS_REFCURSOR
                              )
  AS
    v_sql       VARCHAR2(4000);
    v_where     VARCHAR2(4000);
    v_start_num NUMBER := 1;
    v_end_num   NUMBER:= 14;
    i           NUMBER := 0;
    
    TYPE bind_typ IS TABLE OF VARCHAR2(4000) INDEX BY PLS_INTEGER;
    bind_tab  bind_typ;
    
  BEGIN
    v_sql := 'SELECT E.Empno AS Employee_ID,
                    E.Ename As Employee_Name,
                    E.Job   AS Job,
                    E.Sal   AS Salary,
                    E.Comm  AS Commission,
                    E.Sal + NVL(E.Comm, 0) AS Total_Salary,
                    (E.Sal + NVL(E.Comm, 0) * 12 AS Salary_per_Anum,
                    D.Dname AS Department_Name
              FROM Emp E
              LEFT JOIN Dept D ON (E.Deptno = D.Deptno)';
              
    IF p_empno IS NOT NULL THEN
      v_where := ' AND E.Empno = :empno';
      i := i + 1;
      bind_tab(i) := TRIM(p_empno);
    END IF;
    
    IF p_sal IS NOT NULL THEN
      v_where := ' AND E.Sal = :sal';
      i := i + 1;
      bind_tab(i) := TRIM(p_sal);
    END IF;
    
    IF p_deptno IS NOT NULL THEN
      v_where := ' AND E.Deptno = :deptno';
      i := i + 1;
      bind_tab(i) := TRIM(p_deptno);
    END IF;
    
    IF v_where IS NOT NULL THEN
      v_where := v_sql||' WHERE '||SUBSTR(v_where, 5);
    END IF;
    
    v_sql := 'SELECT * 
              FROM (SELECT A.*,DENSE_RANK() OVER (ORDER BY DEPTNO,ROWNUM) Q
                    FROM (' ||v_sql|| ') A 
                   ) WHERE  Q >= :v_start_num 
                     AND Q <= :v_end_num ';
    
    DELETE FROM TEST_TAB;
    INSERT INTO TEST_TAB VALUES(v_sql); COMMIT;
    
    IF bind_tab.FIRST IS NOT NULL THEN
      CASE bind_tab.COUNT
        WHEN 1 THEN OPEN p_result FOR v_sql USING bind_tab(1), v_start_num, v_end_num;
        WHEN 2 THEN OPEN p_result FOR v_sql USING bind_tab(1), bind_tab(2), v_start_num, v_end_num;
        WHEN 3 THEN OPEN p_result FOR v_sql USING bind_tab(1), bind_tab(2), bind_tab(3), v_start_num, v_end_num;
        WHEN 4 THEN OPEN p_result FOR v_sql USING bind_tab(1), bind_tab(2), bind_tab(3), bind_tab(4), v_start_num, v_end_num;
      END CASE;
    ELSE
      OPEN p_result FOR v_sql USING v_start_num, v_end_num;
    END IF;
    
  END search_emp_details;

END collection_pkg;
/
