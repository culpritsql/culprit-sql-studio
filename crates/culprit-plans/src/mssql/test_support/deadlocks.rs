pub const KEY_LOOKUP_XDL: &str = r#"<deadlock-list><deadlock victim="process61">
 <process-list>
  <process id="process61" spid="61" lockMode="S" isolationlevel="read committed (2)" currentdbname="shop" loginname="app" hostname="web-1" clientapp="orders-api" logused="0">
   <executionStack><frame procname="adhoc" line="1">SELECT id, name, segment FROM shop.customers WHERE email = @P1</frame></executionStack>
   <inputbuf>(@P1 nvarchar(200))SELECT id, name, segment FROM shop.customers WHERE email = @P1</inputbuf>
  </process>
  <process id="process64" spid="64" lockMode="X" isolationlevel="read committed (2)" currentdbname="shop" loginname="app" hostname="worker-2" clientapp="billing" logused="1240">
   <executionStack><frame procname="adhoc" line="1">UPDATE shop.customers SET email = @P1 WHERE id = @P2</frame></executionStack>
   <inputbuf />
  </process>
 </process-list>
 <resource-list>
  <keylock hobtid="1" dbid="5" objectname="shop.shop.customers" indexname="ix_customers_email" id="lock1" mode="X">
   <owner-list><owner id="process64" mode="X"/></owner-list>
   <waiter-list><waiter id="process61" mode="S" requestType="wait"/></waiter-list>
  </keylock>
  <keylock hobtid="2" dbid="5" objectname="shop.shop.customers" indexname="PK_customers" id="lock2" mode="S">
   <owner-list><owner id="process61" mode="S"/></owner-list>
   <waiter-list><waiter id="process64" mode="X" requestType="wait"/></waiter-list>
  </keylock>
 </resource-list>
</deadlock></deadlock-list>"#;

pub fn event(at: &str) -> String {
    format!(
        r#"<event name="xml_deadlock_report" timestamp="{at}"><data name="xml_report"><value>{}</value></data></event>"#,
        KEY_LOOKUP_XDL.replace("<deadlock-list>", "").replace("</deadlock-list>", "")
    )
}
