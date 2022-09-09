
CREATE OR REPLACE FUNCTION send_insert_notification () RETURNS TRIGGER AS
$$

BEGIN
  PERFORM pg_notify('cache_notification', row_to_json(new)::text);


RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER send_select_notification_trigger
    AFTER INSERT OR UPDATE ON users FOR EACH ROW
    EXECUTE PROCEDURE send_insert_notification();
