import QtQuick 2.12
import QtQuick.Window 2.12
import QtQuick.Controls 2.12


import "timelines"

Window {
    id: root
    width: 1000
    height: 580
    visible: true
    title: qsTr("PyMMDT Dashboard")

    Rectangle {
        id: appContainer
        color: "#282a36"
        anchors.fill: parent

        Rectangle {
            id: topBar
            height: 0.05*parent.height
            color: "#282a36"
            anchors.right: parent.right
            anchors.left: parent.left
            anchors.top: parent.top

            Button {
                id: playPauseButton
                x: 0.5*parent.width
                width: 50
                icon.source: Manager.is_play ? "resources/icons8-play-30.png" : "resources/icons8-pause-30.png"
                icon.color: "#f8f8f2"
                anchors.bottom: parent.bottom
                anchors.bottomMargin: 6
                anchors.top: parent.top
                anchors.topMargin: 6
                display: AbstractButton.IconOnly
                background: Rectangle {
                    color: "transparent"
                }
                onClicked: Manager.play_pause()
                onDoubleClicked: Manager.play_pause() // Necessary for capturing fast clicks!
            }

            Button {
                id: appButton
                x: 0.1*parent.width
                width: 100
                display: AbstractButton.TextOnly
                transformOrigin: Item.Center
                font.pointSize: 13
                anchors.bottom: parent.bottom
                anchors.left: parent.left
                anchors.top: parent.top
                background: Rectangle {
                    color: "transparent"
                }
                Text {
                    anchors.fill: parent
                    horizontalAlignment: Text.AlignHCenter
                    verticalAlignment: Text.AlignVCenter
                    color: "#f8f8f2"
                    text: qsTr("PyMMDT")
                }
            }

            Button {
                id: settingsButton
                x: 0.9*parent.width
                width: 100
                icon.source: "resources/icons8-settings-50.png"
                icon.color: "#f8f8f2"
                anchors.right: parent.right
                anchors.bottom: parent.bottom
                anchors.top: parent.top
                background: Rectangle {
                    color: "transparent"
                }

            }

            Button {
                id: userButton
                x: 0.15 * parent.width
                width: 100
                anchors.bottom: parent.bottom
                anchors.top: parent.top
                background: Rectangle {
                    color: "transparent"
                }
                Text {
                    anchors.fill: parent
                    horizontalAlignment: Text.AlignHCenter
                    verticalAlignment: Text.AlignVCenter
                    color: "#f8f8f2"
                    text: qsTr("User-Sort")
                }
            }

            Button {
                id: entryButton
                x: 0.35 * parent.width
                width: 100
                anchors.top: parent.top
                anchors.bottom: parent.bottom
                background: Rectangle {
                    color: "transparent"
                }
                Text {
                    anchors.fill: parent
                    horizontalAlignment: Text.AlignHCenter
                    verticalAlignment: Text.AlignVCenter
                    color: "#f8f8f2"
                    text: qsTr("Entry-Sort")
                }
            }
        }

        // Timetrack (container for Timelines)
        Rectangle {
            id: timeTrack
            // height: timetrackView.contentHeight
            height: 200
            color: "#282a36"
            anchors.bottom: parent.bottom
            anchors.right: parent.right
            anchors.left: parent.left
            
            ListView {
                id: timetrackView
                anchors.fill: timeTrack
                spacing: 0
                orientation: ListView.Vertical
                model: Manager.timetrack_model
                delegate: BaseTimelineDelegate{}
            }

            Rectangle {
                y: 0
                x: SlidingBar.state * (timeTrack.width-30-width) + 30
                width: 5
                height: parent.height
                visible: Manager.data_is_loaded
                color: "orange"
            }
        }

        Rectangle {
            id: content
            color: "#44475a"
            anchors.top: topBar.bottom
            anchors.bottom: timeTrack.top
            anchors.right: parent.right
            anchors.left: parent.left

            ScrollView {
                anchors.fill: parent
                clip: true

                Loader {
                    id: pageLoader
                    anchors.fill: parent
                    source: Qt.resolvedUrl("pages/" + Manager.page + ".qml")
                }
            }
        }
    }
}
