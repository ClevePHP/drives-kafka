<?php

namespace ClevePHP\Drives\Queues\kafka;

use Core\Util\Logger;

class Kafkaqueues {
	static $driveObject;
	static $config = [ ];
	static $offsets = [ ];
	public static function driveObject($configs) {
		self::$config = $configs;
		return (new self ());
	}
	public static function push($data, $toppic = "list") {
		$conf = [ 
				"toppic" => $toppic,
				"group_id" => isset ( self::$config ['group_id'] ) ? self::$config ['group_id'] : "T-001" 
		];
		if (self::$config) {
			$conf = array_merge ( $conf, self::$config );
		}
		if (is_array ( $data )) {
			$data = json_encode ( $data );
		}
		$config = (\ClevePHP\Drives\Queues\kafka\Config::getInstance ())->loadConfig ( $conf );
		return (\ClevePHP\Drives\Queues\kafka\Producers::getInstance ())->config ( $config )->produce ( $data );
	}
	public static function pop(callable $callback, $toppic = "list") {
		$conf = [ 
				"toppic" => $toppic,
				"group_id" => isset ( self::$config ['group_id'] ) ? self::$config ['group_id'] : "T-001" 
		];
		if (self::$config) {
			$conf = array_merge ( $conf, self::$config );
		}
		$config = (\ClevePHP\Drives\Queues\kafka\Config::getInstance ())->loadConfig ( $conf );
		
		// consumerLow
		if ($config->consumerModel == 0) {
			(\ClevePHP\Drives\Queues\kafka\Consumer::getInstance ())->config ( $config )->consumer ( function ($message) use($callback, $config) {
				$offsetDatas = self::$offsets [$config->toppic] ?? [ ];
				if ($offsetDatas) {
					if (in_array ( $message->offset, $offsetDatas ) || isset ( $offsetDatas [$message->offset] )) {
						echo PHP_EOL . "repeat--data----" . PHP_EOL;
						return;
					}
					if (count ( $offsetDatas ) > 1000) {
						array_pop ( $offsetDatas );
					}
				}
				$offsetDatas [$message->offset] = $message->offset;
				self::$offsets [$config->toppic] = $offsetDatas;
				if ($message && property_exists ( $message, "payload" )) {
					$jsonData = @json_decode ( $message->payload, TRUE );
					if ($message->payload && $jsonData) {
						$message->payload = $jsonData;
					}
				}
				($callback instanceof \Closure) && call_user_func ( $callback, $message );
			} );
		} elseif ($config->consumerModel == 1) {
			(\ClevePHP\Drives\Queues\kafka\Consumer::getInstance ())->config ( $config )->consumerLow ( function ($message) use($callback) {
				if ($message && property_exists ( $message, "payload" )) {
					$jsonData = @json_decode ( $message->payload, TRUE );
					if ($message->payload && $jsonData) {
						$message->payload = $jsonData;
					}
				}
				($callback instanceof \Closure) && call_user_func ( $callback, $message );
			} );
		} elseif ($config->consumerModel == 2) {
			Logger::echo ( "consumerModel=2.........." );
			(\ClevePHP\Drives\Queues\kafka\Consumer::getInstance ())->config ( $config )->consumerPop ( function ($message) use($callback) {
				if ($message && property_exists ( $message, "payload" )) {
					$jsonData = @json_decode ( $message->payload, TRUE );
					if ($message->payload && $jsonData) {
						$message->payload = $jsonData;
					}
				}
				($callback instanceof \Closure) && call_user_func ( $callback, $message );
			} );
		}
	}
}
