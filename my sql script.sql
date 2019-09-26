WITH f AS (
WITH t AS (
WITH rs AS
  (
      SELECT
        date_trunc('week', rs.created_at) :: DATE AS DATA,
        rs.*,
        row_number()
        OVER (
          PARTITION BY rs.ticket_id
          ORDER BY rs.created_at, rs.id )         AS rn_first,
        sum(CASE WHEN rs.status IN (1, 7)
          THEN 1
            ELSE 0 END)
        OVER (
          PARTITION BY rs.ticket_id
          ORDER BY rs.created_at ASC, rs.id ASC ) AS cycle
      FROM contact_center.ticket_status_history rs
      WHERE rs.ticket_id IN
            --  AND
            (SELECT DISTINCT ticket_id
             FROM contact_center.ticket_status_history
             WHERE status = 2 AND
                   created_at :: DATE BETWEEN (date_trunc('week', current_date) - INTERVAL '10 weeks')::DATE AND (date_trunc('week', current_date))::DATE -- AND rs.ticket_id = 686555
            )
  )
SELECT
  rs.DATA                                 Неделя,
  rs.ticket_id,
  rs.created_at                        AS date_created,
  rs.status                            AS status_created,
  case when rs.status=7 then 1
    end as status_reopened
  ,lead(rs.created_at)
  OVER (
    PARTITION BY rs.ticket_id
    ORDER BY rs.created_at,
      rs.id )                          AS date_accepted,
  lead(rs.status)
  OVER (
    PARTITION BY rs.ticket_id
    ORDER BY rs.created_at,
      rs.id )                          AS status_accepted,
  lead(rs.user_id)
  OVER (
    PARTITION BY rs.ticket_id
    ORDER BY rs.created_at,
      rs.id )                          AS user_accepted,
  roles,
  th.name                                 "Тема сообщения начальная",
  cl.roles as Роль,
  u.id,
  sv.name                              AS Система,
  ch.name AS Канал,
  CASE WHEN sv.name = 'ДомКлик' THEN
      CASE WHEN (replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) ILIKE '%ROLE_POL_MARKETER%'
      OR replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) ILIKE '%ROLE_POL_PARTNER_HEAD%'
    --  OR replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) ILIKE '%ROLE_ABSTRACT_PARLINE_USER%'
      OR replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) ILIKE '%ROLE_POL_PRIVATE_AGENT%'
      OR replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) ILIKE '%AGENT%'
      OR replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) ILIKE '%ROLE_ONREG_DEVELOPER_HEAD%'
      OR replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) ILIKE '%ROLE_DEVELOPER%'
      OR replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) ILIKE '%ROLE_POL_AGENT%'
      OR replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) ILIKE '%ROLE_POL_OFFICE_HEAD%'
      OR replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) ILIKE '%ROLE_LKP_ACCREDITATION%')
      AND replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) NOT ILIKE '%MOIK%'
      AND replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) NOT ILIKE '%MIK%'
      AND replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) NOT ILIKE '%VSP%'
      AND replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) NOT ILIKE '%MOIK%'
      AND replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) NOT ILIKE '%norp%'
      AND replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) NOT ILIKE '%TerMan%'
      AND replace(replace(ARRAY[roles]::text, '{',''), '}', '')::VARCHAR(200) NOT ILIKE '%COMPAS%' then 'ДК партнеры'
      ELSE (CASE WHEN cl.cas_id is null THEN 'Неавторизованные' ELSE  'ДК клиенты' END )
      END
      END as ДК,
   CASE WHEN sv.name = 'ДомКлик' THEN
   CASE WHEN ch.name ILIKE '%ДомКлик%' THEN 'Web'
          WHEN ch.name ILIKE '%Mobile Android%' THEN 'Mobile Android'
          WHEN ch.name ILIKE '%Mobile IOS%' THEN 'Mobile IOS' END
   END AS ДК_приложение
  ,coalesce(h.name, 'без хештега')         Хештег,
  trim(TRAILING ' ' FROM coalesce(u.last_name, '') || ' ' || coalesce(u.first_name, '') || ' ' ||
                         coalesce(u.middle_name,
                                  '')) AS ФИО_сотрудника,
  coalesce(cl.TB, 'Не определен')         ТБ
FROM rs
  LEFT JOIN contact_center."user" u ON u.id = rs.user_id
  LEFT JOIN contact_center.ticket t ON t.id = rs.ticket_id
  LEFT JOIN contact_center.service sv ON sv.id = t.service_id
  LEFT JOIN contact_center.client cl ON cl.id = t.client_id
  LEFT JOIN contact_center.theme th ON th.id = t.theme_id
  LEFT JOIN contact_center.hashtag h ON h.id = t.hashtag_id
  LEFT JOIn contact_center.channel ch ON ch.id=t.channel_id
WHERE
rs.created_at :: DATE BETWEEN (date_trunc('week', current_date) - INTERVAL '10 weeks')::DATE AND date_trunc('week',                                                                                                             current_date)::DATE
       AND
      sv.name in ('Парлайн','ДК','Компас','ДК ВСП', 'CRM2.0', 'МОИК', 'Eva') AND t.jira_id is null
      --AND ch.id=6
   --  AND ARRAY[roles]<>'{}'
      AND (u.id IN (27, 24, 129, 28, 3, 11, 25, 29, 370, 368, 23, 30, 7, 105, 375, 26) OR u.id IS NULL) --and t.description NOT ILIKE '%Тест%'
)
SELECT
    t.Неделя,
    t.ticket_id,
    t.status_created,
    t.date_created,
    t.status_reopened,
    t.date_accepted,
    t.status_accepted,
    t.user_accepted,
    t.roles,
    t.ФИО_сотрудника,
    t.Роль,
    t.Система,
    t.Канал,
    t.ДК,
    t.ДК_приложение,
    t.ТБ,
    t.Хештег,
    t."Тема сообщения начальная",
    sum(1.0000000000 * (extract(EPOCH FROM t.date_accepted) - extract(EPOCH FROM t.date_created)) /
    (60 * 60)) OVER (PARTITION BY t.ticket_id) AS "Время принятия задачи"
, row_number() OVER (PARTITION BY t.ticket_id ORDER BY t.date_accepted desc) as rn
  FROM t
  WHERE status_accepted = 2 and status_created in (1,7) and t.Неделя<date_trunc('week', now())::DATE --and t.description NOT ILIKE '%Тест%'
 -- AND user_accepted IN (27, 24, 129, 28, 3, 11, 25, 29, 370, 368, 23, 30, 7, 105, 375, 26)
) SELECT f.Неделя, f.ticket_id, 1 as "Все созданные тикеты", f.status_reopened, f.date_created, f.status_created, f.date_accepted, f.status_accepted, f.Роль
    ,f.ФИО_сотрудника, f.Канал, f.Система, f.ДК, f.ДК ,f.ТБ, f.Хештег, f."Тема сообщения начальная", f."Время принятия задачи"
  FROM f WHERE f.rn=1
ORDER BY 1;