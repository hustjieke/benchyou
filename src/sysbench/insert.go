/*
 * benchyou
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package sysbench

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	//"strings"
	"sync"
	"sync/atomic"
	"time"
	"xcommon"
	"xworker"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

// Insert tuple.
type Insert struct {
	stop     bool
	requests uint64
	mu       sync.Mutex
	t_nano   uint64
	conf     *xcommon.Conf
	workers  []xworker.Worker
	lock     sync.WaitGroup
}

// NewInsert creates the new insert handler.
func NewInsert(conf *xcommon.Conf, workers []xworker.Worker) xworker.Handler {
	return &Insert{
		conf:    conf,
		workers: workers,
	}
}

// Run used to start the worker.
func (insert *Insert) Run() {
	threads := len(insert.workers)
	insert.t_nano = uint64(time.Now().UnixNano())
	for i := 0; i < threads; i++ {
		insert.lock.Add(1)
		go insert.Insert(&insert.workers[i], threads, i)
	}
}

// Stop used to stop the worker.
func (insert *Insert) Stop() {
	insert.stop = true
	insert.lock.Wait()
}

// Rows returns the row numbers.
func (insert *Insert) Rows() uint64 {
	return atomic.LoadUint64(&insert.requests)
}

// Insert used to execute the insert query.
/*
func (insert *Insert) Insert(worker *xworker.Worker, num int, id int) {
	session := worker.S
	bs := int64(math.MaxInt64) / int64(num)
	lo := bs * int64(id)
	hi := bs * int64(id+1)
	columns1 := "k,c,pad"
	columns2 := "k,c,pad,id"
	valfmt1 := "(%v,'%s', '%s'),"
	valfmt2 := "(%v,'%s', '%s', %v),"

	for !insert.stop {
		var sql, value string
		buf := common.NewBuffer(256) // TODO(gry): 放大

		table := rand.Int31n(int32(worker.N))
		if insert.conf.Random {
			sql = fmt.Sprintf("insert into benchyou%d(%s) values", table, columns2)
		} else {
			sql = fmt.Sprintf("insert into benchyou%d(%s) values", table, columns1)
		}

		// pack requests
		for n := 0; n < insert.conf.RowsPerInsert; n++ {
			pad := xcommon.RandString(xcommon.Padtemplate)
			c := xcommon.RandString(xcommon.Ctemplate)

			if insert.conf.Random {
				value = fmt.Sprintf(valfmt2,
					xcommon.RandInt64(lo, hi),
					c,
					pad,
					xcommon.RandInt64(lo, hi),
				)
			} else { // TODO(gry): SELECT LENGTH(CONCAT(current_timestamp(6), RANDOM_BYTES(486)));
				value = fmt.Sprintf(valfmt1,
					xcommon.RandInt64(lo, hi),
					c,
					pad,
				)
			}
			buf.WriteString(value)
		}
		// -1 to trim right ','
		vals, err := buf.ReadString(buf.Length() - 1)
		if err != nil {
			log.Panicf("insert.error[%v]", err)
		}
		sql += vals

		t := time.Now()
		// Txn start.
		mod := worker.M.WNums % uint64(insert.conf.BatchPerCommit)
		if insert.conf.BatchPerCommit > 1 {
			if mod == 0 {
				if _, err := session.Exec("begin"); err != nil {
					log.Panicf("insert.error[%v]", err)
				}
			}
		}
		// XA start.
		if insert.conf.XA {
			xaStart(worker, hi, lo)
		}
		if _, err := session.Exec(sql); err != nil {
			log.Panicf("insert.error[%v]", err)
		}
		// XA end.
		if insert.conf.XA {
			xaEnd(worker)
		}
		// Txn end.
		if insert.conf.BatchPerCommit > 1 {
			if mod == uint64(insert.conf.BatchPerCommit-1) {
				if _, err := session.Exec("commit"); err != nil {
					log.Panicf("insert.error[%v]", err)
				}
			}
		}
		elapsed := time.Since(t)

		// stats
		nsec := uint64(elapsed.Nanoseconds())
		worker.M.WCosts += nsec
		if worker.M.WMax == 0 && worker.M.WMin == 0 {
			worker.M.WMax = nsec
			worker.M.WMin = nsec
		}

		if nsec > worker.M.WMax {
			worker.M.WMax = nsec
		}
		if nsec < worker.M.WMin {
			worker.M.WMin = nsec
		}
		worker.M.WNums++
		atomic.AddUint64(&insert.requests, 1)
	}
	insert.lock.Done()
}
*/

// Insert used to execute the insert query.
func (insert *Insert) Insert(worker *xworker.Worker, num int, id int) {
	session := worker.S
	bs := int64(math.MaxInt64) / int64(num)
	lo := bs * int64(id)
	hi := bs * int64(id+1)
	// case1
	seq_id_prefix := "4100001200001010"
	valfmt := "('%s',41,'1d0fbc0c1bf0781eb1dbc38605b37b36','4100001200001010011','4100001200002020',1,'384','106','3E23D3DA99','818D22EEA9','2024-03-30 11:53:24',1070,33805,32735,33805,'1','2','4100001200002020','1070','33805','32735','蒙ECJTFC_5','%s','晋ZSXUFU','6','2','2','10','583DBB8F','09','6735D8OQAYLC7608','50McW7xFQKGLDC8uD88F','iRnUYEnXiFONtPPjomEG','1',2,'20230930',1,'20230930',1359,2,'410000120000101001','71EC6DCF65','2024-03-30 11:53:24','3','014102109731478393509520240308210709','05873519626852787754','B177F31E7D','2024-03-30 11:53:24','20639FB7','67664605417251467358','2262BEACDF3CDDEE',105,20240330,20240330,36,'1','蒙ECJTFC_5','晋ZSXUFU','6','2',1,23,2,56612,2838,2149,2298,'4104','04','蒙ECJTFC_5','晋ZSXUFU','6','2',20240330,20240330,1,'1','05873519626852787754',66748,6377,9,'郑州东收费站4','66748',66748,6377,3,'计费信息1','计费信息2','计费信息3',1,2,'2','0503246B',1,'41000012000010100112024033011000010','41000012000010100112024033011000010',1,1,1,'41000012000010100112024033011000010','41000012000010100112024033011000010','2024-03-30 11:53:24','2024-03-30 11:53:24','2024-03-30 11:53:24','2024-03-30 11:53:24','2','2024-03-30 11:53:24','2024-03-30 11:53:24','%d','16','16','5F0C9C',4,'CFA2F0B86CC8CC1F','DFF67AFBCF41F0ED',6,2,4313,5291,4690,3126,2773,4861,3,2,2,2,3,2,2,2,3,'8FAD0B23',3,2,2,2,'2','42AB808E8AFE2187','2024-03-30 11:53:24',3,'01','B552A3CCD4F883A8',5638,5572,4477,4979,'73F14F554BEDE9712880',1,'73F14F554BEDE9712880',20514,4313,5291,4690,0,0,'74B6F342367181CA6CCF',2773,4861,3,'2',20514,3,3,'217A974E592CDBCCCB9D',1,1,'83A28EF78588DAE9',1,3,'83A28EF78588DAE9','2024-03-30 11:53:24',1,05,20230930,3),"
	//valfmt := "('%d',41,'1d0fbc0c1bf0781eb1dbc38605b37b36','4100001200001010011','4100001200002020',1,'384','106','3E23D3DA99','818D22EEA9','2024-03-30 11:53:24',1070,33805,32735,33805,'1','2','4100001200002020','1070','33805','32735','蒙ECJTFC_5','%s','晋ZSXUFU','6','2','2','10','583DBB8F','09','6735D8OQAYLC7608','50McW7xFQKGLDC8uD88F','iRnUYEnXiFONtPPjomEG','1',2,'20230930',1,'20230930',1359,2,'410000120000101001','71EC6DCF65','2024-03-30 11:53:24','3','014102109731478393509520240308210709','05873519626852787754','B177F31E7D','2024-03-30 11:53:24','20639FB7','67664605417251467358','2262BEACDF3CDDEE',105,20240330,20240330,36,'1','蒙ECJTFC_5','晋ZSXUFU','6','2',1,23,2,56612,2838,2149,2298,'4104','04','蒙ECJTFC_5','晋ZSXUFU','6','2',20240330,20240330,1,'1','05873519626852787754',66748,6377,9,'郑州东收费站4','66748',66748,6377,3,'计费信息1','计费信息2','计费信息3',1,2,'2','0503246B',1,'41000012000010100112024033011000010','41000012000010100112024033011000010',1,1,1,'41000012000010100112024033011000010','41000012000010100112024033011000010','2024-03-30 11:53:24','2024-03-30 11:53:24','2024-03-30 11:53:24','2024-03-30 11:53:24','2','2024-03-30 11:53:24','2024-03-30 11:53:24','%d','16','16','5F0C9C',4,'CFA2F0B86CC8CC1F','DFF67AFBCF41F0ED',6,2,4313,5291,4690,3126,2773,4861,3,2,2,2,3,2,2,2,3,'8FAD0B23',3,2,2,2,'2','42AB808E8AFE2187','2024-03-30 11:53:24',3,'01','B552A3CCD4F883A8',5638,5572,4477,4979,'73F14F554BEDE9712880',1,'73F14F554BEDE9712880',20514,4313,5291,4690,0,0,'74B6F342367181CA6CCF',2773,4861,3,'2',20514,3,3,'217A974E592CDBCCCB9D',1,1,'83A28EF78588DAE9',1,3,'83A28EF78588DAE9','2024-03-30 11:53:24',1,05,20230930,3),"
	//var mu sync.Mutex

	for !insert.stop {
		var sql, value string
		buf := common.NewBuffer(1024 * 1024 * 8) // TODO(gry): 256 buffer 放大, 不然 176 列不够

		// TODO(gry) here we can mock product tables here, use N to judge.
		sql = fmt.Sprintf("insert into tdts_gantry_free_trans(`id`,`province_type`,`msg_id`,`gantry_id`,`interval_id`,`computer_order`,`hour_batch_no`,`gantry_order_num`,`gantry_hex`,`gantry_hex_opposite`,`trans_time`,`pay_fee`,`fee`,`discount_fee`,`trans_fee`,`media_type`,`obu_sign`,`toll_interval_id`,`pay_fee_group`,`fee_group`,`discount_fee_group`,`vehicle_plate`,`licence_code`,`licence_code2`,`licence_color`,`vehicle_type`,`identify_vehicle_type`,`vehicle_class`,`tac`,`trans_type`,`terminal_no`,`terminal_transno`,`trans_no`,`service_type`,`algorithm_identifier`,`key_version`,`antenna_id`,`rate_version`,`consume_time`,`pass_state`,`en_toll_lane_id`,`en_toll_station_hex`,`en_time`,`en_lane_type`,`pass_id`,`media_num`,`last_gantry_hex`,`last_gantry_time`,`obu_mac`,`obu_issue_id`,`obu_sn`,`obu_version`,`obu_start_date`,`obu_end_date`,`obu_electrical`,`obu_state`,`obu_vehicle_plate`,`obu_licence_code`,`obu_licence_color`,`obu_vehicle_type`,`vehicle_user_type`,`vehicle_seat`,`axle_count`,`total_weight`,`vehicle_length`,`vehicle_width`,`vehicle_hight`,`cpu_net_id`,`cpu_issue_id`,`cpu_vehicle_plate`,`cpu_licence_code`,`cpu_licence_color`,`cpu_vehicle_type`,`cpu_start_date`,`cpu_end_date`,`cpu_version`,`cpu_card_type`,`cpu_card_id`,`balance_before`,`balance_after`,`gantry_pass_count`,`gantry_pass_info`,`fee_prov_info`,`fee_sum_local_before`,`fee_sum_local_after`,`fee_calc_result`,`fee_info1`,`fee_info2`,`fee_info3`,`holiday_state`,`trade_result`,`special_type`,`verify_code`,`interrupt_signal`,`vehicle_pic_id`,`vehicle_tail_pic_id`,`match_status`,`valid_status`,`deal_status`,`related_trade_id`,`all_related_trade_id`,`station_db_time`,`station_deal_time`,`station_valid_time`,`station_match_time`,`vehicle_sign`,`receive_time`,`insert_time`,`sub_partition`,`charges_special_type`,`special_type1`,`auth_id`,`fee_calc_special`,`last_gantry_hex_fee`,`last_gantry_hex_pass`,`rate_compute`,`rate_fit_count`,`obu_fee_sum_before`,`obu_fee_sum_after`,`obu_prov_fee_sum_before`,`obu_prov_fee_sum_after`,`card_fee_sum_before`,`card_fee_sum_after`,`no_card_times_before`,`no_card_times_after`,`province_num_before`,`province_num_after`,`obu_total_trade_succ_num_before`,`obu_total_trade_succ_num_after`,`obu_prov_trade_succ_num_before`,`obu_prov_trade_succ_num_after`,`obu_trade_result`,`obu_verify_code`,`trade_type`,`obu_info_type_read`,`obu_info_type_write`,`obu_pass_state`,`fee_vehicle_type`,`obu_last_gantry_hex`,`obu_last_gantry_time`,`transaction_type`,`gantry_type`,`toll_interval_sign`,`obu_pay_fee_sum_before`,`obu_pay_fee_sum_after`,`obu_discount_fee_sum_before`,`obu_discount_fee_sum_after`,`trade_read_ciphertext`,`read_ciphertext_verify`,`trade_write_ciphertext`,`fee_mileage`,`obu_mileage_before`,`obu_mileage_after`,`prov_min_fee`,`fee_spare1`,`fee_spare2`,`fee_spare3`,`fee_prov_begin_hex`,`obu_prov_pay_fee_sum_before`,`obu_prov_pay_fee_sum_after`,`path_fit_flag`,`fee_calc_specials`,`pay_fee_prov_sum_local`,`pcrsu_version`,`gantry_pass_info_after`,`update_result`,`cpc_fee_trade_result`,`fee_prov_ef04`,`fit_prov_flag`,`gantry_pass_count_before`,`fee_prov_begin_hex_fit`,`fee_prov_begin_time_fit`,`is_fix_data`,`rsu_manu_id`,`fee_data_version`,`vehicle_class_num`) values")

		// pack requests
		for n := 0; n < insert.conf.RowsPerInsert; n++ {
			car := xcommon.RandString(xcommon.Cartemplate)
			sub_partition := xcommon.RandInt64(0, 1000)

			if insert.conf.Random {
				t_nano := strconv.FormatInt(xcommon.RandInt64(lo, hi), 10) + strconv.FormatInt(time.Now().UnixNano(), 10)
				value = fmt.Sprintf(valfmt,
					t_nano,
					car,
					sub_partition, // TODO(gry): rm hard code
				)
			} else { // produce varchar with order: SELECT LENGTH(CONCAT(current_timestamp(6), RANDOM_BYTES(486)));
				insert.mu.Lock()
				atomic.AddUint64(&insert.t_nano, 1)
				t_nano := seq_id_prefix + strconv.FormatUint(insert.t_nano, 10)
				value = fmt.Sprintf(valfmt,
					//insert.t_nano,
					t_nano,
					car,
					sub_partition,
				)
				insert.mu.Unlock()
			}
			// log.Println("gry----sql: ", value)
			//log.Println("gry---长度: ", len(strings.Split(sql, ",")))
			//log.Println("gry---列表: ", strings.Split(sql, ","))
			buf.WriteString(value)
		}
		// -1 to trim right ','
		vals, err := buf.ReadString(buf.Length() - 1)
		if err != nil {
			log.Panicf("insert.error[%v]", err)
		}
		sql += vals

		t := time.Now()
		// Txn start.
		mod := worker.M.WNums % uint64(insert.conf.BatchPerCommit)
		if insert.conf.BatchPerCommit > 1 {
			if mod == 0 {
				if _, err := session.Exec("begin"); err != nil {
					log.Panicf("insert.error[%v]", err)
				}
			}
		}
		// XA start.
		if insert.conf.XA {
			xaStart(worker, hi, lo)
		}
		if _, err := session.Exec(sql); err != nil {
			// log.Panicf("insert.error[%v]", err)
			atomic.AddUint64(&insert.requests, 1)
			log.Println("gry---requests failed: ", insert.requests)
		}
		// XA end.
		if insert.conf.XA {
			xaEnd(worker)
		}
		// Txn end.
		if insert.conf.BatchPerCommit > 1 {
			if mod == uint64(insert.conf.BatchPerCommit-1) {
				if _, err := session.Exec("commit"); err != nil {
					log.Panicf("insert.error[%v]", err)
				}
			}
		}
		elapsed := time.Since(t)

		// stats
		nsec := uint64(elapsed.Nanoseconds())
		worker.M.WCosts += nsec
		if worker.M.WMax == 0 && worker.M.WMin == 0 {
			worker.M.WMax = nsec
			worker.M.WMin = nsec
		}

		if nsec > worker.M.WMax {
			worker.M.WMax = nsec
		}
		if nsec < worker.M.WMin {
			worker.M.WMin = nsec
		}
		worker.M.WNums++
		//atomic.AddUint64(&insert.requests, 1)
		//log.Println("gry---requests: ", insert.requests)
	}
	insert.lock.Done()

}

func xaStart(worker *xworker.Worker, hi int64, lo int64) {
	session := worker.S
	worker.XID = fmt.Sprintf("BXID-%v-%v", time.Now().Format("20060102150405"), (rand.Int63n(hi-lo) + lo))
	start := fmt.Sprintf("xa start '%s'", worker.XID)
	if _, err := session.Exec(start); err != nil {
		log.Panicf("xa.start..error[%v]", err)
	}
}

func xaEnd(worker *xworker.Worker) {
	session := worker.S
	end := fmt.Sprintf("xa end '%s'", worker.XID)
	if _, err := session.Exec(end); err != nil {
		log.Panicf("xa.end.error[%v]", err)
	}
	prepare := fmt.Sprintf("xa prepare '%s'", worker.XID)
	if _, err := session.Exec(prepare); err != nil {
		log.Panicf("xa.prepare.error[%v]", err)
	}
	commit := fmt.Sprintf("xa commit '%s'", worker.XID)
	if _, err := session.Exec(commit); err != nil {
		log.Panicf("xa.commit.error[%v]", err)
	}
}

// TODO(gry) here we can mock product tables here, use N to judge.
//var sql = fmt.Sprintf("insert into tdts_gantry_free_trans(`gantry_hex_opposite`,`trans_time`,`pay_fee`,`fee`,`discount_fee`,`trans_fee`,`media_type`,`obu_sign`,`toll_interval_id`,`pay_fee_group`,`fee_group`,`discount_fee_group`,`vehicle_plate`,`licence_code`,`licence_code2`,`licence_color`,`vehicle_type`,`identify_vehicle_type`,`vehicle_class`,`tac`,`trans_type`,`terminal_no`,`terminal_transno`,`trans_no`,`service_type`,`algorithm_identifier`,`key_version`,`antenna_id`,`rate_version`,`consume_time`,`pass_state`,`en_toll_lane_id`,`en_toll_station_hex`,`en_time`,`en_lane_type`,`pass_id`,`media_num`,`last_gantry_hex`,`last_gantry_time`,`obu_mac`,`obu_issue_id`,`obu_sn`,`obu_version`,`obu_start_date`,`obu_end_date`,`obu_electrical`,`obu_state`,`obu_vehicle_plate`,`obu_licence_code`,`obu_licence_color`,`obu_vehicle_type`,`vehicle_user_type`,`vehicle_seat`,`axle_count`,`total_weight`,`vehicle_length`,`vehicle_width`,`vehicle_hight`,`cpu_net_id`,`cpu_issue_id`,`cpu_vehicle_plate`,`cpu_licence_code`,`cpu_licence_color`,`cpu_vehicle_type`,`cpu_start_date`,`cpu_end_date`,`cpu_version`,`cpu_card_type`,`cpu_card_id`,`balance_before`,`balance_after`,`gantry_pass_count`,`gantry_pass_info`,`fee_prov_info`,`fee_sum_local_before`,`fee_sum_local_after`,`fee_calc_result`,`fee_info1`,`fee_info2`,`fee_info3`,`holiday_state`,`trade_result`,`special_type`,`verify_code`,`interrupt_signal`,`vehicle_pic_id`,`vehicle_tail_pic_id`,`match_status`,`valid_status`,`deal_status`,`related_trade_id`,`all_related_trade_id`,`station_db_time`,`station_deal_time`,`station_valid_time`,`station_match_time`,`vehicle_sign`,`receive_time`,`insert_time`,`sub_partition`,`charges_special_type`,`special_type1`,`auth_id`,`fee_calc_special`,`last_gantry_hex_fee`,`last_gantry_hex_pass`,`rate_compute`,`rate_fit_count`,`obu_fee_sum_before`,`obu_fee_sum_after`,`obu_prov_fee_sum_before`,`obu_prov_fee_sum_after`,`card_fee_sum_before`,`card_fee_sum_after`,`no_card_times_before`,`no_card_times_after`,`province_num_before`,`province_num_after`,`obu_total_trade_succ_num_before`,`obu_total_trade_succ_num_after`,`obu_prov_trade_succ_num_before`,`obu_prov_trade_succ_num_after`,`obu_trade_result`,`obu_verify_code`,`trade_type`,`obu_info_type_read`,`obu_info_type_write`,`obu_pass_state`,`fee_vehicle_type`,`obu_last_gantry_hex`,`obu_last_gantry_time`,`transaction_type`,`gantry_type`,`toll_interval_sign`,`obu_pay_fee_sum_before`,`obu_pay_fee_sum_after`,`obu_discount_fee_sum_before`,`obu_discount_fee_sum_after`,`trade_read_ciphertext`,`read_ciphertext_verify`,`trade_write_ciphertext`,`fee_mileage`,`obu_mileage_before`,`obu_mileage_after`,`prov_min_fee`,`fee_spare1`,`fee_spare2`,`fee_spare3`,`fee_prov_begin_hex`,`obu_prov_pay_fee_sum_before`,`obu_prov_pay_fee_sum_after`,`path_fit_flag`,`fee_calc_specials`,`pay_fee_prov_sum_local`,`pcrsu_version`,`gantry_pass_info_after`,`update_result`,`cpc_fee_trade_result`,`fee_prov_ef04`,`fit_prov_flag`,`gantry_pass_count_before`,`fee_prov_begin_hex_fit`,`fee_prov_begin_time_fit`,`is_fix_data`,`rsu_manu_id`,`fee_data_version`,`vehicle_class_num`) values")
